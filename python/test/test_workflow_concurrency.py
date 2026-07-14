from gen_thrift.api import ttypes

from ai.chronon import group_by, join, model, staging_query


def _event_source(table="events"):
    return ttypes.Source(
        events=ttypes.EventSource(
            table=table,
            query=ttypes.Query(
                startPartition="2024-01-01",
                selects={"user_id": "user_id", "value": "value"},
                timeColumn="ts",
            ),
        )
    )


def test_join_sets_workflow_concurrency():
    gb = group_by.GroupBy(
        sources=_event_source("right_events"),
        keys=["user_id"],
        aggregations=[],
    )
    conf = join.Join(
        left=_event_source("left_events"),
        right_parts=[ttypes.JoinPart(groupBy=gb)],
        workflow_concurrency=42,
    )

    assert conf.metaData.executionInfo.workflowConcurrency == 42


def test_group_by_sets_workflow_concurrency():
    conf = group_by.GroupBy(
        sources=_event_source(),
        keys=["user_id"],
        aggregations=[],
        workflow_concurrency=43,
    )

    assert conf.metaData.executionInfo.workflowConcurrency == 43


def test_staging_query_sets_workflow_concurrency():
    conf = staging_query.StagingQuery(
        query="SELECT 1 AS value",
        workflow_concurrency=44,
    )

    assert conf.metaData.executionInfo.workflowConcurrency == 44


def test_model_sets_workflow_concurrency():
    conf = model.Model(
        version="v1",
        workflow_concurrency=45,
    )

    assert conf.metaData.executionInfo.workflowConcurrency == 45


def test_model_transforms_sets_workflow_concurrency():
    model_conf = model.Model(version="v1")
    model_conf.metaData.name = "test.model"
    conf = model.ModelTransforms(
        sources=[_event_source()],
        models=[model_conf],
        version=1,
        workflow_concurrency=46,
    )

    assert conf.metaData.executionInfo.workflowConcurrency == 46
