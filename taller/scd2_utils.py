from typing import List, Optional, Tuple
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as F



def build_tracking_change_condition_sql(
    tracking_cols: List[str],
    source_alias: str = "update",
    target_alias: str = "target"
) -> str:
    """
    Construye una condición SQL para Delta Merge.

    Retorna algo como:
    NOT (target.`col1` <=> update.`col1`)
    OR NOT (target.`col2` <=> update.`col2`)
    """

    if not tracking_cols:
        raise ValueError("tracking_cols no puede estar vacío.")

    return " OR ".join(
        [
            f"NOT ({target_alias}.`{col_name}` <=> {source_alias}.`{col_name}`)"
            for col_name in tracking_cols
        ]
    )

def add_scd2_metadata(          
        source_df: DataFrame,
        keys: List[str],
        tracking_cols: List[str] = None,
        sequence_by: str = None,
        start_col: str = "__start_at",
        end_col: str = "__end_at",) -> None:
    
    

    if tracking_cols != None:
        _window_first_update = Window.partitionBy(
            keys + tracking_cols
        ).orderBy(F.col(sequence_by).asc())
        source_df = (
            source_df.withColumn(
                "first_update", F.row_number().over(_window_first_update)
            )
            .where("first_update = 1")
            .drop("first_update")
        )

    __window_end_at = Window.partitionBy(keys).orderBy(
        F.col(sequence_by).desc()
    )
    return source_df.dropDuplicates(subset= keys + tracking_cols).withColumns(
        {
            start_col: F.col(sequence_by),
            end_col: F.lag(sequence_by).over(__window_end_at),
        }
    )


class SCD2:
    def __init__(
        self,
        source_df: DataFrame,
        sink_table_name : str,
        keys: List[str],
        tracking_cols: List[str] = None,
        sequence_by: str = None,
        start_col: str = "__start_at",
        end_col: str = "__end_at",
        
    ) -> None:
        self.keys = keys
        self.source_df = source_df
        self.sink_table_name = sink_table_name
        self.source_columns = source_df.columns
        self.tracking_cols = (
            tracking_cols
            if tracking_cols != None
            else list(
                filter(lambda x: x not in [start_col, end_col]), self.source_columns
            )
        )
        self.sequence_by = sequence_by
        self.start_col = start_col
        self.end_col = end_col



    def __upsert_scd2_table(self,microbatch_df,batch_id):
        
        if microbatch_df.isEmpty():
            return
        
        

        spark = microbatch_df.sparkSession

        source_df = add_scd2_metadata(microbatch_df,
                                self.keys,
                                self.tracking_cols,
                                self.sequence_by,
                                self.start_col,
                                self.end_col).alias("update")

        if not spark.catalog.tableExists(self.sink_table_name):
            (source_df.withColumn('mode',F.lit('append_new')).write.format("delta").mode("overwrite").saveAsTable(self.sink_table_name))

        else:
            target_table = DeltaTable.forName(spark,self.sink_table_name)
            target_df = target_table.toDF().alias("target")
            source_df = source_df


            merge_condition = " AND ".join(
                [f"target.{k} = update.{k}" for k in self.keys]
                + [f"target.{self.end_col} IS NULL"]
            )
            tracking_condition = ' ('  + build_tracking_change_condition_sql(self.tracking_cols) + ')'
            condition = f"(target.{self.end_col} IS NULL AND target.{self.sequence_by} < update.{self.sequence_by} ) AND " + tracking_condition
            target_table.alias('target').merge(
                source_df.where(f'{self.end_col} IS NULL'), merge_condition
            ).whenMatchedUpdate(
                condition=condition,
                set={f"target.{self.end_col}": f"update.{self.start_col}",
                     "mode":F.lit('update')}
            ).execute()

            #Registros nuevos
            source_df.withColumn('mode',F.lit('append_new')).join(target_df.select(self.keys).distinct(), on=self.keys, how="left_anti").write.mode(
                "append"
            ).format("delta").saveAsTable(self.sink_table_name)

            last_update_target_df =  target_df.withColumn('last_update',F.row_number().over(Window.partitionBy(self.keys).orderBy(F.col(self.end_col).desc())))\
                                        .where(f"last_update = 1")\
                                        .drop('last_update').alias('target')
            #Actualizacoines
            source_df.where(f'{self.end_col} IS NULL').join( last_update_target_df, on=self.keys, how="inner")\
                .where(tracking_condition)\
                .where(f"update.{self.sequence_by} > target.{self.sequence_by}")\
                    .select('update.*')\
                        .dropDuplicates()\
                        .withColumn('mode',F.lit('append_update'))\
                .write.mode(
                "append"
            ).format("delta").saveAsTable(self.sink_table_name)

    def stream_update_bronze(self,checkpoint):
        """
        Creates the Bronze table if needed and starts a streaming write to upsert new data using availableNow trigger.
        Returns:
        StreamingQuery: The streaming query object.
        """
        return (
        self.source_df
        .writeStream.trigger(availableNow=True)
        .foreachBatch(lambda df, batch_id: self.__upsert_scd2_table( df, batch_id))
        .option("checkpointLocation",   checkpoint )
        .outputMode("append")
        .start()
        )