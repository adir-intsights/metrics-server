import asyncio
import argparse
import dataclasses
import datetime
import time

import tabulate
import termcolor
import google.cloud.monitoring_v3


async def main():
    args = parse_args()

    utilization_monitor = UtilizationMonitor(
        project=args.project,
        namespaces=args.namespaces,
        duration_days=args.duration_days,
    )

    await utilization_monitor.print_utilization()


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--project',
        required=True,
    )

    parser.add_argument(
        '--namespaces',
        nargs='+',
    )

    parser.add_argument(
        '--duration-days',
        type=int,
        default=7,
    )

    return parser.parse_args()


class UtilizationMonitor:
    def __init__(
        self,
        project,
        namespaces,
        duration_days,
    ):
        self.metrics_client = google.cloud.monitoring_v3.services.metric_service.MetricServiceAsyncClient()
        self.project = project
        self.namespaces = namespaces

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        self.duration = int(datetime.timedelta(days=duration_days).total_seconds())
        self.interval = google.cloud.monitoring_v3.TimeInterval(
            {
                'end_time': {
                    'seconds': seconds,
                    'nanos': nanos,
                },
                'start_time': {
                    'seconds': (seconds - self.duration),
                    'nanos': nanos,
                },
            },
        )

        self.request_semaphore = asyncio.Semaphore(
            value=10,
        )

        self.container_metrics = {}

    async def print_utilization(
        self,
    ):
        await self.query_metrics()

        container_metrics = list(self.container_metrics.values())
        container_metrics.sort(
            key=lambda m: m.unutilized_memory,
            reverse=True,
        )
        table = []

        for metrics in container_metrics:
            avg_memory_request_utilization = percentage_gradient(
                value=metrics.avg_memory_request_utilization,
            )

            unutilized_memory = mb_gradient(
                value=metrics.unutilized_memory,
            )

            table.append(
                [
                    metrics.namespace,
                    f'{metrics.top_level_controller_name}/{metrics.container_name}',
                    f'{metrics.memory_request / 1024 ** 2:.0f} MB',
                    f'{metrics.avg_memory_usage / 1024 ** 2:.0f} MB',
                    f'{metrics.max_memory_usage / 1024 ** 2:.0f} MB',
                    avg_memory_request_utilization,
                    unutilized_memory,
                ],
            )

        total_avg_memory_request_utilization = percentage_gradient(
            value=sum(
                metrics.avg_memory_request_utilization for metrics in container_metrics
            ) / len(container_metrics),
        )

        total_avg_memory_usage = sum(
            metrics.avg_memory_usage for metrics in container_metrics
        ) / len(container_metrics)

        total_avg_memory_request = sum(
            metrics.memory_request for metrics in container_metrics
        )

        total_unutilized_memory = sum(
            metrics.unutilized_memory for metrics in container_metrics
        )

        table.append(
            [
                'Total',
                '',
                f'{total_avg_memory_request / 1024 ** 3:.0f} GB',
                f'{total_avg_memory_usage / 1024 ** 2:.0f} MB',
                '',
                total_avg_memory_request_utilization,
                f'{total_unutilized_memory / 1024 ** 3:.0f} GB',
            ],
        )

        print(
            tabulate.tabulate(
                tabular_data=table,
                headers=[
                    'Namespace',
                    'Deployment/Container',
                    'Mem Req',
                    'Avg Mem Usage',
                    'Max Mem Usage',
                    'Avg Mem Req Util',
                    'Unutilized Mem',
                ],
            ),
        )

    async def query_metrics(
        self,
    ):
        await asyncio.gather(
            self.query_avg_memory_request_utilization(),
            self.query_max_memory_usage(),
            self.query_avg_memory_usage(),
            self.query_memory_request(),
        )

        for container_key, metrics in list(self.container_metrics.items()):
            if 'overprovisioner' in metrics.container_name:
                del self.container_metrics[container_key]

    async def query(
        self,
        aligner,
        reducer,
        query_filter,
    ):
        if self.namespaces:
            query_filter += ' AND (' + ' OR '.join(
                f'resource.labels.namespace_name = "{namespace}"' for namespace in self.namespaces
            ) + ')'

        aggregation = google.cloud.monitoring_v3.Aggregation(
            {
                'alignment_period': {
                    'seconds': self.duration,
                },
                'per_series_aligner': aligner,
                'cross_series_reducer': reducer,
                'group_by_fields': [
                    'resource.labels.namespace_name',
                    'resource.container_name',
                    'metadata.system_labels.top_level_controller_name'
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

    async def query_avg_memory_request_utilization(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter=f'metric.type = "kubernetes.io/container/memory/request_utilization"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_memory_request_utilization = metrics.points[0].value.double_value

    async def query_max_memory_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter=f'metric.type = "kubernetes.io/container/memory/used_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.max_memory_usage = metrics.points[0].value.int64_value

    async def query_avg_memory_usage(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            query_filter=f'metric.type = "kubernetes.io/container/memory/used_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.avg_memory_usage = metrics.points[0].value.double_value

    async def query_memory_request(
        self,
    ):
        results = await self.query(
            aligner=google.cloud.monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            reducer=google.cloud.monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            query_filter=f'metric.type = "kubernetes.io/container/memory/request_bytes"',
        )

        async for metrics in results:
            container = self.upsert_container(
                metrics=metrics,
            )

            container.memory_request = metrics.points[0].value.int64_value

def percentage_gradient(
    value,
):
    if value < 0.3:
        color = 'red'
    elif value < 0.6:
        color = 'yellow'
    else:
        color = 'green'

    return termcolor.colored(
        text=f'{value:.0%}',
        color=color,
    )


def mb_gradient(
    value,
):
    value_in_mb = int(value / 1024 ** 2)

    if value_in_mb > 200:
        color = 'red'
    elif value_in_mb > 100:
        color = 'yellow'
    else:
        color = 'green'

    return termcolor.colored(
        text=f'{value_in_mb} MB',
        color=color,
    )

@dataclasses.dataclass
class ContainerMetrics:
    container_name: str
    top_level_controller_name: str
    namespace: str
    memory_request: int = 0
    avg_memory_usage: float = 0
    avg_memory_request_utilization: float = 0
    max_memory_usage: int = 0

    @property
    def unutilized_memory(
        self,
    ):
        return max(
            0,
            self.memory_request - self.max_memory_usage,
        )


if __name__ == '__main__':
    asyncio.run(main())
