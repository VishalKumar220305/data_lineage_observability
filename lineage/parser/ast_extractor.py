"""
ast_extractor.py

Statically parses a PySpark job script (without running it) and extracts
source -> target table lineage by tracing which read() calls a saveAsTable()
call's DataFrame variable actually derives from.

How it works, in plain terms:
  1. Walk the script's statements in order (inside the top-level function,
     typically `run()`).
  2. Every time a variable is assigned the result of a read call
     (spark.read.csv(...), spark.read.table(...), spark.table(...)), record
     that variable as "derived from" that source.
  3. Every time a variable is assigned the result of a transformation
     (.select(...), .join(...), .withColumn(...), etc.) on an already-tracked
     variable, the new variable inherits that variable's sources -- and if
     the transformation references OTHER tracked variables (e.g. a join
     partner), those sources are folded in too. This is what correctly
     produces fan-in for build_warehouse.py's join-heavy script.
  4. When a .saveAsTable("TargetName") call is found, we trace back to the
     DataFrame variable it was called on, look up its tracked sources, and
     emit one edge per source -> TargetName.
  5. If a saveAsTable() target can't be traced back to any known source
     (e.g. the source table name was built dynamically instead of as a
     plain string), it's recorded as UNRESOLVED instead of guessed at --
     this is intentional. An unresolved case should become a manual entry
     in lineage_overrides, not a silent wrong guess.

This is deliberately variable-level tracing, not just "every read in the
file maps to every write in the file" -- that cruder approach would wrongly
claim, e.g., that stg_sellers feeds Dim_Customer just because both appear
in build_warehouse.py. Precision here is what makes the parser's output
trustworthy enough to build a blast-radius graph on top of.
"""

import ast


class LineageExtractionResult:
    def __init__(self):
        self.edges = []        # list of (source, target) tuples, fully resolved
        self.unresolved = []   # list of dicts: {"target": ..., "reason": ...}


class _LineageVisitor:
    def __init__(self):
        self.var_sources = {}      # variable name -> set of source identifiers
        self.module_constants = {}  # module-level NAME -> string value
        self.result = LineageExtractionResult()

    # ---- entry point ----------------------------------------------------
    def process_module(self, tree: ast.Module):
        # First, collect module-level string constants (e.g.
        # TARGET_TABLE = "stg_customers") so references to them inside
        # run() can be resolved -- this is an extremely common, perfectly
        # reasonable pattern and should NOT be treated as "unresolved."
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 \
                    and isinstance(node.targets[0], ast.Name) \
                    and isinstance(node.value, ast.Constant) \
                    and isinstance(node.value.value, str):
                self.module_constants[node.targets[0].id] = node.value.value

        # Prefer tracing inside a top-level `run()` function, since that's
        # the convention every job script in this project follows. Fall
        # back to module-level statements if there's no such function.
        run_func = None
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == "run":
                run_func = node
                break

        body = run_func.body if run_func else tree.body
        self._process_body(body)

    # ---- statement-level walking -----------------------------------------
    def _process_body(self, body):
        for stmt in body:
            self._process_stmt(stmt)

    def _process_stmt(self, stmt):
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 \
                and isinstance(stmt.targets[0], ast.Name):
            var_name = stmt.targets[0].id
            sources = self._trace_sources(stmt.value)
            if sources:
                self.var_sources[var_name] = sources

        elif isinstance(stmt, ast.Expr):
            self._check_for_write(stmt.value)

        # Recurse into compound statements (try/except, if, with) since our
        # jobs wrap their body in try/finally.
        elif isinstance(stmt, ast.Try):
            self._process_body(stmt.body)
            for handler in stmt.handlers:
                self._process_body(handler.body)
            self._process_body(stmt.orelse)
            self._process_body(stmt.finalbody)
        elif isinstance(stmt, ast.If):
            self._process_body(stmt.body)
            self._process_body(stmt.orelse)
        elif isinstance(stmt, ast.With):
            self._process_body(stmt.body)

    # ---- expression-level tracing -----------------------------------------
    def _trace_sources(self, node):
        """Returns the set of source identifiers a DataFrame expression
        ultimately derives from, or None if it isn't a traceable DataFrame
        expression at all (e.g. a plain string or number)."""

        if isinstance(node, ast.Call):
            read_source = self._match_read_call(node)
            if read_source is not None:
                return {read_source}
            if read_source is _UNRESOLVED_READ:
                # It IS a read call, but the argument wasn't a plain string
                # (e.g. built from a variable/f-string) -- can't resolve.
                return {_UNRESOLVED_MARKER}

            sources = set()
            # The receiver of a method call, e.g. df in df.select(...)
            if isinstance(node.func, ast.Attribute):
                recv_sources = self._trace_sources(node.func.value)
                if recv_sources:
                    sources |= recv_sources
            # Arguments too -- catches df.join(other_df, ...)
            for arg in node.args:
                arg_sources = self._trace_sources(arg)
                if arg_sources:
                    sources |= arg_sources
            return sources if sources else None

        elif isinstance(node, ast.Name):
            return self.var_sources.get(node.id)

        elif isinstance(node, ast.Attribute):
            return self._trace_sources(node.value)

        elif isinstance(node, ast.Subscript):
            # e.g. orders["order_id"] -- lineage flows from the base df
            return self._trace_sources(node.value)

        return None

    def _match_read_call(self, node):
        """Returns the source string if this call is spark.read.csv(...),
        spark.read.table(...), or spark.table(...); returns _UNRESOLVED_READ
        if it's a read call but the arg wasn't a literal string; returns
        None if it isn't a read call at all."""
        func = node.func
        if not isinstance(func, ast.Attribute):
            return None

        is_spark_read_dot = (
            func.attr in ("csv", "table")
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "read"
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "spark"
        )
        is_spark_table_direct = (
            func.attr == "table"
            and isinstance(func.value, ast.Name)
            and func.value.id == "spark"
        )

        if is_spark_read_dot or is_spark_table_direct:
            arg_value = self._first_str_arg(node)
            return arg_value if arg_value is not None else _UNRESOLVED_READ

        return None

    def _first_str_arg(self, node):
        if not node.args:
            return None
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
        if isinstance(arg, ast.Name) and arg.id in self.module_constants:
            return self.module_constants[arg.id]
        return None

    def _check_for_write(self, node):
        if not isinstance(node, ast.Call):
            return
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "saveAsTable":
            target = self._first_str_arg(node)
            sources = self._trace_sources(func.value)  # e.g. df.write.mode(...)

            if target is None:
                self.result.unresolved.append({
                    "target": "<unknown - dynamic table name>",
                    "reason": "saveAsTable() argument was not a plain string literal",
                })
                return

            if not sources:
                self.result.unresolved.append({
                    "target": target,
                    "reason": "could not trace the DataFrame back to a read() call",
                })
                return

            if _UNRESOLVED_MARKER in sources:
                self.result.unresolved.append({
                    "target": target,
                    "reason": "source table name was not a plain string literal "
                              "(built dynamically)",
                })
                sources = sources - {_UNRESOLVED_MARKER}

            for source in sources:
                self.result.edges.append((source, target))


_UNRESOLVED_READ = object()   # sentinel: "this IS a read call, but unresolved"
_UNRESOLVED_MARKER = "<unresolved>"


def extract_lineage_from_source(source_code: str) -> LineageExtractionResult:
    """Parses PySpark source code and returns a LineageExtractionResult."""
    tree = ast.parse(source_code)
    visitor = _LineageVisitor()
    visitor.process_module(tree)
    return visitor.result


def extract_lineage_from_file(filepath: str) -> LineageExtractionResult:
    with open(filepath, "r", encoding="utf-8") as f:
        source_code = f.read()
    return extract_lineage_from_source(source_code)
