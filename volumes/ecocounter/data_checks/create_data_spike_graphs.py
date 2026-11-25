import pandas as pd
import os

import random
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_data_spike_graphs(conn_id, outpath):

    # We need to return the paths for the send_msg_with_images utility
    list_of_graphs = []

    engine = PostgresHook(conn_id).get_sqlalchemy_engine()

    # grab all relevant sites
    complete_sites = pd.read_sql("SELECT DISTINCT (site_id) FROM ecocounter.flows WHERE flow_id IN (SELECT flow_id FROM dboucau.open_data_pull_data_spikes())", 
                        engine)
    # identify spikes from function
    spikes_and_zeros = pd.read_sql("SELECT * FROM dboucau.open_data_pull_data_spikes()", 
                        engine)
    
    counts_query = """
        SELECT
            datetime_bin::date AS date_bin,
            flow_id,
            SUM(calibrated_volume) AS daily_volume,
            flow_direction,
            mode_counted,
            CASE WHEN flow_direction = direction_main THEN 'flow' ELSE 'counterflow' END AS flow_type
        FROM ecocounter.counts
        JOIN ecocounter.flows 
            USING (flow_id)
        WHERE datetime_bin >= now() - interval '2 months'
        AND site_id = {site_id_}
        GROUP BY (1, flow_id, flow_direction, mode_counted, flow_type)
        ORDER BY 1

    """

    colourscheme = [
        '#2E91E5', '#E15F99', '#1CA71C', '#FB0D0D',
        '#DA16FF', '#222A2A', '#B68100', '#750D86',
        '#EB663B', '#511CFB', '#00A08B', '#FB00D1',
        '#FC0080', '#B2828D', '#6C7C32', '#778AAE',
        '#862A16', '#A777F1', '#620042', '#1616A7',
        '#DA60CA', '#6C4516', '#0D2A63', '#AF0038'
    ]

    for site_id in complete_sites['site_id']:

        with engine.connect() as conn:
            site_info = conn.execute(
                text(f"""
                    SELECT row_to_json(st)
                    FROM (
                        SELECT *
                        FROM ecocounter.sites
                        WHERE site_id = {int(site_id)}
                    ) st
                """)
            ).scalar_one()

        daily_flow_counts = pd.read_sql(
            counts_query.format(site_id_=int(site_id)),
            con=engine
        )

        colours = random.sample(
            colourscheme,
            len(daily_flow_counts.flow_id.unique())
        )

        fig, ax = plt.subplots(
            figsize=(12, 6),
            constrained_layout=True
        )

        i = 0

        grouped = (
            daily_flow_counts
            .groupby(
                ['flow_id', 'flow_direction', 'flow_type', 'mode_counted']
            )['daily_volume']
            .mean()
            .sort_values(ascending=False)
        )

        for (
            flow_id,
            flow_direction,
            flow_type,
            mode_counted
        ), mean_volume in grouped.items():

            colour = colours[i]
            i += 1

            temp_volumes = (
                daily_flow_counts[
                    daily_flow_counts.flow_id == flow_id
                ]
                .sort_values('date_bin')
            )

            linestyle = '--' if flow_type == 'counterflow' else '-'

            label = (
                f"{flow_direction} : {int(flow_id)} "
                f"({mode_counted}) ({flow_type})"
            )

            # Main line
            ax.plot(
                temp_volumes['date_bin'],
                temp_volumes['daily_volume'],
                color=colour,
                linestyle=linestyle,
                linewidth=2,
                marker='o',
                markersize=4,
                alpha=0.5,
                label=label
            )

            # Mean line
            ax.axhline(
                y=mean_volume,
                color=colour,
                linestyle=':',
                alpha=0.5
            )

            # Mean annotation on top flow
            if i == 0:
                ax.text(
                    temp_volumes['date_bin'].max(),
                    mean_volume,
                    f"mean volume",
                    color=colour,
                    fontsize=8,
                    ha='right',
                    va='bottom'
                )

            temp_spikes = spikes_and_zeros[
                (spikes_and_zeros.flow_id == flow_id)
                & (spikes_and_zeros.anomaly == 'data_spike')
            ]

            temp_zeros = spikes_and_zeros[
                (spikes_and_zeros.flow_id == flow_id)
                & (spikes_and_zeros.anomaly == '0_volume')
            ]

            # Spikes
            ax.scatter(
                temp_spikes['date_bin'],
                temp_spikes['daily_volume'],
                marker='^',
                s=150,
                color=colour,
                alpha=0.5,
                label=None
            )

            # Zeros
            ax.scatter(
                temp_zeros['date_bin'],
                temp_zeros['daily_volume'],
                marker='x',
                s=150,
                color=colour,
                alpha=0.5,
                label=None
            )

        # Custom legend entries for anomaly markers
        custom_handles = [
            Line2D(
                [0], [0],
                marker='^',
                color='grey',
                linestyle='None',
                markersize=10,
                label='data spike (+/- 3 * std)'
            ),
            Line2D(
                [0], [0],
                marker='x',
                color='grey',
                linestyle='None',
                markersize=10,
                label='volume=0'
            )
        ]

        handles, labels = ax.get_legend_handles_labels()

        ax.legend(
            handles + custom_handles,
            labels + [
                'data spike (+/- 3 * std)',
                'volume=0'
            ],
            fontsize=8,
            bbox_to_anchor=(1.02, 1),
            loc='upper left'
        )

        ax.set_title(
            f"{site_info['site_description']} - "
            f"site_id: {site_info['site_id']}"
        )

        ax.set_ylabel("Daily Volume")
        ax.set_xlabel("Date")

        fig.autofmt_xdate()

        file_path = os.path.join(
            outpath,
            f"{int(site_id)}.png"
        )

        list_of_graphs.append(file_path)

        plt.savefig(
            file_path,
            dpi=150,
            bbox_inches='tight'
        )

        plt.close(fig)
        
    return list_of_graphs