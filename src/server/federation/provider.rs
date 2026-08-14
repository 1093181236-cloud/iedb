// A table provider that unions persisted Parquet (ListingTable) with
// agent buffer data (MemTable) at scan time.
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

pub struct FederatedTableProvider {
    pub listing: Arc<dyn TableProvider>,
    pub memory: Arc<MemTable>,
    pub schema: SchemaRef,
}

#[async_trait]
impl TableProvider for FederatedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let p1 = self.listing.scan(state, projection, filters, limit).await?;
        let p2 = self.memory.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(UnionExec::new(vec![p1, p2])))
    }
}
