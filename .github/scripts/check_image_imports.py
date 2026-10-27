"""Fail when code imports a package that its image does not install.

CI runs this with only the image's packages installed, before the test tools add packages the
image lacks. It reads every import in the folder's code without running it, including imports
inside functions, and skips `if TYPE_CHECKING:` blocks because those never run.
Usage: python -I check_image_imports.py <folder>, such as src/iisa or cronjobs/compute_scores
"""

import ast
import importlib.util
import sys
from pathlib import Path


class _ImportCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        self.names.update(alias.name.split(".")[0] for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module:
            self.names.add(node.module.split(".")[0])

    def visit_If(self, node: ast.If) -> None:
        test = node.test
        name = test.id if isinstance(test, ast.Name) else getattr(test, "attr", None)
        if name != "TYPE_CHECKING":
            self.visit(node.test)
            for child in node.body:
                self.visit(child)
        for child in node.orelse:
            self.visit(child)


def main(job_dir: Path) -> int:
    sources = sorted(job_dir.glob("*.py"))
    local = {path.stem for path in sources} | {path.name for path in job_dir.iterdir()}
    missing: dict[str, list[str]] = {}
    for path in sources:
        collector = _ImportCollector()
        collector.visit(ast.parse(path.read_text(), filename=str(path)))
        for name in sorted(collector.names - local):
            if importlib.util.find_spec(name) is None:
                missing.setdefault(name, []).append(path.name)
    for name, files in missing.items():
        print(f"::error::{', '.join(files)} imports {name}, which the image does not install")
    print(f"Checked the imports of {len(sources)} files in {job_dir}: {len(missing)} missing")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
