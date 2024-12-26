use alloc::string::ToString;
use core::convert::Into;
use common_daft_config::PyDaftPlanningConfig;
use daft_dsl::python::PyExpr;
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use pyo3::prelude::*;
use daft_core::python::PyDataType;

use crate::{catalog::SQLCatalog, functions::SQL_FUNCTIONS, planner::SQLPlanner};
use crate::functions::register_sql_udf_to_functions;

use common_resource_request::ResourceRequest;
use crate::modules::python::SQLPythonFunction;

#[pyclass]
pub struct SQLFunctionStub {
    name: String,
    docstring: String,
    arg_names: Vec<&'static str>,
}

#[pymethods]
impl SQLFunctionStub {
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.name.clone())
    }

    #[getter]
    fn docstring(&self) -> PyResult<String> {
        Ok(self.docstring.clone())
    }

    #[getter]
    fn arg_names(&self) -> PyResult<Vec<&'static str>> {
        Ok(self.arg_names.clone())
    }
}

#[pyfunction]
pub fn sql(
    sql: &str,
    catalog: PyCatalog,
    daft_planning_config: PyDaftPlanningConfig,
) -> PyResult<PyLogicalPlanBuilder> {
    let mut planner = SQLPlanner::new(catalog.catalog);
    let plan = planner.plan_sql(sql)?;
    Ok(LogicalPlanBuilder::new(plan, Some(daft_planning_config.config)).into())
}

#[pyfunction]
pub fn sql_expr(sql: &str) -> PyResult<PyExpr> {
    let expr = crate::planner::sql_expr(sql)?;
    Ok(PyExpr { expr })
}


#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn register_sql_udf(
    name: &str,
    inner: PyObject,
    return_dtype: PyDataType,
    init_args: PyObject,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
    concurrency: Option<usize>,
) {
    let func = SQLPythonFunction{
        name: name.to_string().into(),
        inner: inner.into(),
        return_dtype: return_dtype.dtype,
        init_args: init_args.into(),
        resource_request,
        batch_size,
        concurrency
    };
    register_sql_udf_to_functions(name, func);
}

#[pyfunction]
pub fn list_sql_functions() -> Vec<SQLFunctionStub> {
    SQL_FUNCTIONS
        .map
        .keys()
        .cloned()
        .map(|name| {
            let (docstring, args) = SQL_FUNCTIONS.docsmap.get(&name).unwrap();
            SQLFunctionStub {
                name,
                docstring: docstring.to_string(),
                arg_names: args.to_vec(),
            }
        })
        .collect()
}

/// PyCatalog is the Python interface to the Catalog.
#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PyCatalog {
    catalog: SQLCatalog,
}

#[pymethods]
impl PyCatalog {
    /// Construct an empty PyCatalog.
    #[staticmethod]
    pub fn new() -> Self {
        Self {
            catalog: SQLCatalog::new(),
        }
    }

    /// Register a table with the catalog.
    pub fn register_table(&mut self, name: &str, dataframe: &mut PyLogicalPlanBuilder) {
        let plan = dataframe.builder.build();
        self.catalog.register_table(name, plan);
    }

    /// Copy from another catalog, using tables from other in case of conflict
    pub fn copy_from(&mut self, other: &Self) {
        self.catalog.copy_from(&other.catalog);
    }

    /// __str__ to print the catalog's tables
    fn __str__(&self) -> String {
        format!("{:?}", self.catalog)
    }
}

impl Default for PyCatalog {
    fn default() -> Self {
        Self::new()
    }
}
