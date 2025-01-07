use alloc::string::String;
use alloc::sync::Arc;
use core::todo;
use sqlparser::ast::FunctionArg;
use common_resource_request::ResourceRequest;
use daft_core::prelude::DataType;
use daft_dsl::{ExprRef};
use daft_dsl::functions::python::{MaybeInitializedUDF, PythonUDF, RuntimePyObject};
use crate::error::SQLPlannerResult;
use super::SQLModule;
use crate::functions::{SQLFunction, SQLFunctions};
use crate::SQLPlanner;

pub struct SQLModulePython;

impl SQLModule for SQLModulePython {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Python as f;
        // TODO
    }
}

pub struct SQLPythonFunction {
    pub name: Arc<String>,
    pub inner: RuntimePyObject,
    pub return_dtype: DataType,
    pub init_args: RuntimePyObject,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<usize>,
}

impl SQLFunction for SQLPythonFunction {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        let expressions = inputs
            .iter()
            .map(|arg| planner.plan_function_arg(arg))
            .collect::<crate::error::SQLPlannerResult<Vec<_>>>()?;
        println!("{:?}", expressions);
        /*
        Ok(Arc::new(Expr::Function {
            func: FunctionExpr::Python(PythonUDF {
                name: self.name.clone(),
                func: MaybeInitializedUDF::Uninitialized { inner: self.inner.clone(), init_args: self.init_args.clone() },
                bound_args,
                num_expressions: expressions.len(),
                return_dtype: self.return_dtype.clone(),
                resource_request: self.resource_request.clone(),
                batch_size: self.batch_size,
                concurrency: self.concurrency,
            }),
            inputs: expressions.into(),
        }));
        */
        todo!()
    }
}