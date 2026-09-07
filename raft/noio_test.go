package raft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// forbiddenImports is the mechanical form of the project's central claim: the
// consensus core cannot read a clock and cannot perform I/O, because it cannot
// reach anything that would let it.
var forbiddenImports = map[string]string{
	"time":        "time must arrive as Tick calls, not as a clock reading",
	"math/rand":   "randomness must arrive as the injected Source",
	"crypto/rand": "randomness must arrive as the injected Source",
	"os":          "the core must not touch the operating system",
	"net":         "the core must not open sockets; messages arrive through Step",
	"io":          "the core must not perform I/O; durability goes through Storage",
	"bufio":       "the core must not perform I/O",
	"sync":        "the core is driven from a single loop; synchronisation belongs to the caller",
}

// TestCoreHasNoClockAndNoIO is a source-level check rather than a behavioural
// one. A runtime test can only observe the clock reads that happen to execute;
// this catches the capability as well as the act.
func TestCoreHasNoClockAndNoIO(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the raft package: %v", err)
	}
	files := 0
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			files++
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if why, bad := forbiddenImports[path]; bad {
					t.Errorf("%s imports %q: %s", base(name), path, why)
				}
				if imp.Name != nil && imp.Name.Name == "_" {
					t.Errorf("%s blank-imports %q, which hides a dependency from this check", base(name), path)
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					if id, ok := x.X.(*ast.Ident); ok {
						switch id.Name {
						case "time":
							t.Errorf("%s calls time.%s; the core has no clock", base(name), x.Sel.Name)
						case "rand":
							t.Errorf("%s calls rand.%s; randomness must come from the injected Source", base(name), x.Sel.Name)
						}
					}
				case *ast.FuncDecl:
					// A goroutine would let the core act on its own schedule,
					// which destroys the tick ordering the simulator relies on.
					if x.Type.Results == nil && x.Body != nil {
						ast.Inspect(x.Body, func(inner ast.Node) bool {
							if _, ok := inner.(*ast.GoStmt); ok {
								t.Errorf("%s: %s starts a goroutine; the core must be driven by its caller", base(name), x.Name.Name)
							}
							return true
						})
					}
				}
				return true
			})
		}
	}
	if files < 5 {
		t.Fatalf("only %d non-test source files were scanned; the check is not looking at the whole package", files)
	}
}

func base(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}
