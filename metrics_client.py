import asyncio
import dataclasses
import datetime
import statistics
import time
import typing

import google.cloud.monitoring_v3


@dataclasses.dataclass
class ContainerMetrics:
    container_name: str
    top_level_controller_name: str
    namespace: str
    memory_request: int = 0
    avg_memory_usage: float = 0
    avg_memory_request_utilization: float = 0
    max_memory_usage: int = 0
    unutilized_memory: int = 0
    cpu_request: float = 0
    avg_cpu_usage: float = 0
    avg_cpu_request_utilization: float = 0
    max_cpu_usage: float = 0
    unutilized_cpu: float = 0


class MetricsClient:
    def __init__(
        self,
        project,
        namespaces,
        duration_days,
    ):
        self.metrics_client = google.cloud.monitoring_v3.services.metric_service.MetricServiceAsyncClient()
        self.project = project
        self.namespaces = namespaces

        seconds = int(time.time())
        self.duration = int(
            datetime.timedelta(
                days=duration_days,
            ).total_seconds(),
        )
        self.interval = google.cloud.monitoring_v3.TimeInterval(
            {
                'end_time': {
                    'seconds': seconds,
                    'nanos': 0,
                },
                'start_time': {
                    'seconds': (seconds - self.duration),
                    'nanos': 0,
                },
            },
        )

        self.request_semaphore = asyncio.Semaphore(
            value=10,
        )

        self.container_metrics = {}

    async def query(
        self,
        aligner,
        reducer,
        query_filter,
    ):
        query_filter += ' AND resource.labels.namespace_name != "kube-system"'

        if self.namespaces:
            query_filter += ' AND (' + ' OR '.join(
                f'resource.labels.namespace_name = "{namespace}"' for namespace in self.namespaces
            ) + ')'

        aggregation = google.cloud.monitoring_v3.Aggregation(
            {
                'alignment_period': {
                    'seconds': 60 * 60 * 3,
                },
                'per_series_aligner': aligner,
                'cross_series_reducer': reducer,
                'group_by_fields': [
                    'resource.labels.namespace_name',
                    'resource.container_name',
                    'metadata.system_labels.top_level_controller_name',
                ],
            },
        )

        async with self.request_semaphore:
            return await self.metrics_client.list_time_series(
                request={
                    'name': f'projects/{self.project}',
                    'filter': query_filter,
                    'interval': self.interval,
                    'view': google.cloud.monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    'aggregation': aggregation,
                }
            )

    def upsert_container(
        self,
        metrics,
    ):
        container_key = (
            metrics.resource.labels['namespace_name'],
            metrics.resource.labels['container_name'],
            metrics.metadata.system_labels['top_level_controller_name'],
        )

        container = self.container_metrics.get(container_key)
        if not container:
            self.container_metrics[container_key] = container = ContainerMetrics(
                namespace=metrics.resource.labels['namespace_name'],
                container_name=metrics.resource.labels['container_name'],
                top_level_controller_name=metrics.metadata.system_labels['top_level_controller_name'],
            )

        return container

    async def query_memory_utilization(
        self,
    ) -> typing.List[ContainerMetrics]:
        await asyncio.gather(
            self.query_avg_memory_request_utilization(),
            self.query_max_memory_usage(),
            self.query_avg_memory_usage(),
            self.query_memory_request(),
        )

        for container_key, metrics in list(self.container_metrics.items()):
            if 'overprovisioner' in metrics.container_name:
                del self.container_metrics[container_key]

                continue

            metrics.unutilized_memory = max(
                0,
                metrics.memory_request - metrics.max_memory_usage,
            )

        return list(self.container_metrics.values())

    async def query_avg_memory_request_utilization(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter='metric.type = "kubernetes.io/container/memory/request_utilization"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_memory_request_utilization = statistics.mean([
                point.value.double_value for point in metrics.points
            ])

    async def query_max_memory_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter='metric.type = "kubernetes.io/container/memory/used_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.max_memory_usage = max(
                metrics.points,
                key=lambda point: point.value.int64_value
            ).value.int64_value

    async def query_avg_memory_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter='metric.type = "kubernetes.io/container/memory/used_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_memory_usage = statistics.mean([
                point.value.double_value for point in metrics.points
            ])

    async def query_memory_request(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter='metric.type = "kubernetes.io/container/memory/request_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.memory_request = metrics.points[0].value.int64_value

    async def query_cpu_utilization(
        self,
    ) -> typing.List[ContainerMetrics]:
        await asyncio.gather(
            self.query_avg_cpu_request_utilization(),
            self.query_max_cpu_usage(),
            self.query_avg_cpu_usage(),
            self.query_cpu_request(),
        )

        for container_key, metrics in list(self.container_metrics.items()):
            if 'overprovisioner' in metrics.container_name:
                del self.container_metrics[container_key]

                continue

            metrics.unutilized_cpu = max(
                0,
                metrics.cpu_request - metrics.max_cpu_usage,
            )

        return list(self.container_metrics.values())

    async def query_avg_cpu_request_utilization(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter='metric.type = "kubernetes.io/container/cpu/request_utilization"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_cpu_request_utilization = statistics.mean([
                point.value.double_value for point in metrics.points
            ])

    async def query_max_cpu_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter='metric.type = "kubernetes.io/container/cpu/core_usage_time"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.max_cpu_usage = max(
                metrics.points,
                key=lambda point: point.value.double_value,
            ).value.double_value

    async def query_avg_cpu_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter='metric.type = "kubernetes.io/container/cpu/core_usage_time"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_cpu_usage = metrics.points[0].value.double_value

    async def query_cpu_request(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter='metric.type = "kubernetes.io/container/cpu/request_cores"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.cpu_request = metrics.points[0].value.double_value
