import enum
import typing

import fastapi
import pydantic
import tabulate
import termcolor

from . import gcp_metrics_client


class Settings(
    pydantic.BaseSettings,
):
    project: str


class OutputFormat(
    enum.Enum,
):
    table = 'table'
    json = 'json'
    csv = 'csv'


class MetricType(
    enum.Enum,
):
    memory = 'memory'
    cpu = 'cpu'


settings = Settings()
app = fastapi.FastAPI()


@app.get('/metrics/{metric_type}')
async def get_metrics(
    metric_type: MetricType,
    duration_days: int = 14,
    namespaces: typing.List[str] = fastapi.Query(None),
    output_format: OutputFormat = OutputFormat.table,
):
    client = gcp_metrics_client.MetricsClient(
        project=settings.project,
        namespaces=namespaces,
        duration_days=duration_days,
    )

    if metric_type == MetricType.memory:
        metrics = await client.query_memory_utilization()
    elif metric_type == MetricType.cpu:
        metrics = await client.query_cpu_utilization()

    return format_metrics(
        metric_type=metric_type,
        output_format=output_format,
        metrics=metrics,
    )


def format_metrics(
    metric_type: MetricType,
    output_format: OutputFormat,
    metrics: typing.List[gcp_metrics_client.ContainerMetrics],
):
    if output_format == OutputFormat.table:
        if metric_type == MetricType.memory:
            table = format_memory_metrics_as_table(
                metrics=metrics,
            )
        elif metric_type == MetricType.cpu:
            table = format_cpu_metrics_as_table(
                metrics=metrics,
            )
        return fastapi.Response(
            content=table + '\n',
        )
    elif output_format == OutputFormat.json:
        return metrics


def format_memory_metrics_as_table(
    metrics: typing.List[gcp_metrics_client.ContainerMetrics],
):
    metrics.sort(
        key=lambda m: m.unutilized_memory,
        reverse=True,
    )
    table = []

    for metric in metrics:
        avg_memory_request_utilization = percentage_gradient(
            value=metric.avg_memory_request_utilization,
        )

        unutilized_memory = memory_bytes_gradient(
            value=metric.unutilized_memory,
        )

        table.append(
            [
                metric.namespace,
                f'{metric.top_level_controller_name}/{metric.container_name}',
                f'{metric.memory_request / 1024 ** 2:.0f}M',
                f'{metric.avg_memory_usage / 1024 ** 2:.0f}M',
                f'{metric.max_memory_usage / 1024 ** 2:.0f}M',
                avg_memory_request_utilization,
                unutilized_memory,
            ],
        )

    total_avg_memory_request_utilization = percentage_gradient(
        value=sum(
            metric.avg_memory_request_utilization for metric in metrics
        ) / len(metrics),
    )

    total_avg_memory_usage = sum(
        metric.avg_memory_usage for metric in metrics
    ) / len(metrics)

    total_avg_memory_request = sum(
        metric.memory_request for metric in metrics
    )

    total_unutilized_memory = sum(
        metric.unutilized_memory for metric in metrics
    )

    table.append(
        [
            'Total',
            '',
            f'{total_avg_memory_request / 1024 ** 3:.0f} GB',
            f'{total_avg_memory_usage / 1024 ** 2:.0f}M',
            '',
            total_avg_memory_request_utilization,
            f'{total_unutilized_memory / 1024 ** 3:.0f} GB',
        ],
    )

    return tabulate.tabulate(
        tabular_data=table,
        headers=[
            'Namespace',
            'Deployment/Container',
            'Request',
            'Avg',
            'Max',
            'Avg Utilization',
            'Unutilized',
        ],
    )


def format_cpu_metrics_as_table(
    metrics: typing.List[gcp_metrics_client.ContainerMetrics],
):
    metrics.sort(
        key=lambda m: m.unutilized_cpu,
        reverse=True,
    )
    table = []

    for metric in metrics:
        avg_cpu_request_utilization = percentage_gradient(
            value=metric.avg_cpu_request_utilization,
        )

        unutilized_cpu = cpu_gradient(
            value=metric.unutilized_cpu,
        )

        table.append(
            [
                metric.namespace,
                f'{metric.top_level_controller_name}/{metric.container_name}',
                int(metric.cpu_request * 1000),
                f'{int(metric.avg_cpu_usage * 1000)} ({avg_cpu_request_utilization})',
                int(metric.max_cpu_usage * 1000),
                unutilized_cpu,
            ],
        )

    total_avg_cpu_request_utilization = percentage_gradient(
        value=sum(
            metric.avg_cpu_request_utilization for metric in metrics
        ) / len(metrics),
    )

    total_avg_cpu_usage = sum(
        metric.avg_cpu_usage for metric in metrics
    ) / len(metrics)

    total_avg_cpu_request = sum(
        metric.cpu_request for metric in metrics
    )

    total_unutilized_cpu = sum(
        metric.unutilized_cpu for metric in metrics
    )

    table.append(
        [
            'Total',
            '',
            int(total_avg_cpu_request * 1000),
            int(total_avg_cpu_usage * 1000),
            '',
            # total_avg_cpu_request_utilization,
            int(total_unutilized_cpu * 1000),
        ],
    )

    return tabulate.tabulate(
        tabular_data=table,
        headers=[
            'Namespace',
            'Deployment/Container',
            'Request',
            'Avg',
            'Max',
            'Unutilized',
        ],
    )


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


def memory_bytes_gradient(
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
        text=f'{value_in_mb}M',
        color=color,
    )


def cpu_gradient(
    value,
):
    value_in_milicores = int(value * 1000)

    if value_in_milicores > 1000:
        color = 'red'
    elif value_in_milicores > 100:
        color = 'yellow'
    else:
        color = 'green'

    return termcolor.colored(
        text=value_in_milicores,
        color=color,
    )
