//
// Created by Evert Lammerts on 20/04/2026.
//

#include <nanobind/nanobind.h>

int add(int a, int b) { return a + b; }

NB_MODULE(_duckdb, m) {
	m.def("add", &add);
}
