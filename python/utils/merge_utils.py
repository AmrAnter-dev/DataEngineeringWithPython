def merge_batch(
        spark,
        batch_df,
        target_path,
        merge_column):

    if not DeltaTable.isDeltaTable(spark, target_path):

        (
            batch_df
            .drop("op")
            .write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

        return

    delta = DeltaTable.forPath(
        spark,
        target_path
    )

    (
        delta.alias("t")
        .merge(
            batch_df.alias("s"),
            f"t.{merge_column}=s.{merge_column}"
        )
        .whenMatchedUpdateAll(
            condition="s.op='u'"
        )
        .whenMatchedDelete(
            condition="s.op='d'"
        )
        .whenNotMatchedInsertAll(
            condition="s.op in ('c','r')"
        )
        .execute()
    )
