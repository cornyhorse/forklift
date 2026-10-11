"""Reading what a ``dag.test()`` run did: task states, tries and XComs."""

from __future__ import annotations


def states(dagrun) -> dict:
    return {ti.task_id: ti.state for ti in dagrun.get_task_instances()}


def tries(dagrun) -> dict:
    return {ti.task_id: ti.try_number for ti in dagrun.get_task_instances()}


def xcom(dagrun, task_id: str, key: str = "return_value"):
    from airflow.models.xcom import XComModel
    from airflow.utils.session import create_session

    with create_session() as session:
        query = XComModel.get_many(
            run_id=dagrun.run_id, dag_ids=dagrun.dag_id, task_ids=task_id, key=key
        )
        row = session.scalars(query).first()
        return None if row is None else XComModel.deserialize_value(row)
