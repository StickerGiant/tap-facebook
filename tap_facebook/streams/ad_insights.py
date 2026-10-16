"""Stream class for AdInsights."""

from __future__ import annotations

import time
import typing as t
from collections import deque
from functools import lru_cache

import backoff
import backoff.types
import facebook_business.adobjects.user as fb_user
import pendulum
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsactionstats import AdsActionStats
from facebook_business.adobjects.adshistogramstats import AdsHistogramStats
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream

T = t.TypeVar("T")

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

EXCLUDED_FIELDS = [
    "total_postbacks",
    "adset_end",
    "adset_start",
    "conversion_lead_rate",
    "cost_per_conversion_lead",
    "cost_per_dda_countby_convs",
    "cost_per_one_thousand_ad_impression",
    "cost_per_unique_conversion",
    "creative_media_type",
    "dda_countby_convs",
    "dda_results",
    "instagram_upcoming_event_reminders_set",
    "interactive_component_tap",
    "marketing_messages_cost_per_delivered",
    "marketing_messages_cost_per_link_btn_click",
    "marketing_messages_spend",
    "place_page_name",
    "total_postbacks",
    "total_postbacks_detailed",
    "total_postbacks_detailed_v4",
    "unique_conversions",
    "unique_video_continuous_2_sec_watched_actions",
    "unique_video_view_15_sec",
    "video_thruplay_watched_actions",
    "__module__",
    "__doc__",
    "__dict__",
    "__firstlineno__",  # Python 3.13+
    "__static_attributes__",  # Python 3.13+
    # No longer available >= v19.0: https://developers.facebook.com/docs/marketing-api/marketing-api-changelog/version19.0/
    "age_targeting",
    "gender_targeting",
    "labels",
    "location",
    "estimated_ad_recall_rate_lower_bound",
    "estimated_ad_recall_rate_upper_bound",
    "estimated_ad_recallers_lower_bound",
    "estimated_ad_recallers_upper_bound",
    "marketing_messages_media_view_rate",
    "marketing_messages_phone_call_btn_click_rate",
    "marketing_messages_website_purchase_values",
    "wish_bid",
    # These break requests against v26:
    # - configurable_*: the API rejects the request unless a customization_name filter is supplied.
    "configurable_attribution_action",
    "configurable_attribution_actionvalue",
    "configurable_audience_overlap_reach",
    "configurable_audience_overlap_with_conv_action",
    "configurable_audience_overlap_with_conv_converters",
    "configurable_audience_overlap_with_conv_exposure_cost",
    "configurable_audience_overlap_with_conv_exposure_impressions",
    "configurable_audience_overlap_with_conv_exposure_reach",
    "configurable_placement_ptc_conversions",
    "configurable_placement_ptc_converters",
    "configurable_placement_ptc_reach",
    "configurable_reachbyfrequency_action",
    "configurable_reachbyfrequency_converters_count",
    "configurable_reachbyfrequency_impressions_cost",
    "configurable_reachbyfrequency_impressions_count",
    "configurable_reachbyfrequency_reach",
    # - rejected outright by the API (#2 'Service temporarily unavailable').
    "link_clicks_per_results",
    "opportunity_score_l4",
    "playable_average_game_length",
    "playable_game_start_rate",
    "total_card_view",
    # - These count as 'Custom Analytics metrics' which the API limits to 5 calls per 6 hours, selecting all exhauts that quota
    "advanced_actions_28d_view",
    "advanced_reach_1d_lookback",
    "advanced_reach_28d_lookback",
    "advanced_reach_7d_lookback",
    "anchor_event_attribution_setting",
    "anchor_events_performance_indicator",
    "conversion_leads",
    "converted_product_app_custom_event_fb_mobile_purchase",
    "converted_product_app_custom_event_fb_mobile_purchase_value",
    "converted_product_offline_purchase",
    "converted_product_offline_purchase_value",
    "converted_product_omni_purchase",
    "converted_product_omni_purchase_values",
    "converted_product_website_pixel_purchase",
    "converted_product_website_pixel_purchase_value",
    "converted_promoted_product_app_custom_event_fb_mobile_purchase",
    "converted_promoted_product_app_custom_event_fb_mobile_purchase_value",
    "converted_promoted_product_offline_purchase",
    "converted_promoted_product_offline_purchase_value",
    "converted_promoted_product_omni_purchase",
    "converted_promoted_product_omni_purchase_values",
    "converted_promoted_product_quantity",
    "converted_promoted_product_value",
    "converted_promoted_product_website_pixel_purchase",
    "converted_promoted_product_website_pixel_purchase_value",
    "cost_per_6_sec_video_view",
    "cost_per_message_delivered",
    "cost_per_objective_result",
    "cost_per_result",
    "creative_diversity_data",
    "creative_diversity_label",
    "creative_diversity_score",
    "creative_fatigue_summary",
    "creative_fatigued_ads",
    "instagram_profile_follow",
    "instagram_profile_visits",
    "landing_page_view_actions_per_link_click",
    "landing_page_view_per_link_click",
    "landing_page_view_per_purchase_rate",
    "marketing_messages_click_rate_benchmark",
    "marketing_messages_delivered",
    "marketing_messages_link_btn_click",
    "marketing_messages_quick_reply_btn_click",
    "marketing_messages_read",
    "marketing_messages_read_rate_benchmark",
    "marketing_messages_sent",
    "marketing_messages_spend_currency",
    "messages_delivered",
    "messages_delivered_ctr",
    "msa_seller_budget",
    "multi_event_conversion_attribution_setting",
    "objective_result_rate",
    "objective_results",
    "product_group_retailer_id",
    "product_retailer_id",
    "product_views",
    "purchase_per_landing_page_view",
    "purchases_per_link_click",
    "read_rate",
    "result_rate",
    "result_values_performance_indicator",
    "results",
    "shop_clicks",
    "video_6_sec_watched_actions",
    "video_view_per_impression",
]

SLEEP_TIME_INCREMENT = 5
INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60

# Number of days requested per async report job. The Facebook Insights API returns
# one row per `time_increment` day for the whole `time_range`, so a wider window is
# still daily-grained but collapses ~chunk_days one-day jobs into a single job.
DEFAULT_REPORT_CHUNK_DAYS = 7
# Max async report jobs kept in flight at once. Facebook computes async jobs in
# parallel server-side and throttles per account, so this stays small; tune per
# report via `max_concurrent_jobs` if the account tolerates more (or hits limits).
DEFAULT_MAX_CONCURRENT_JOBS = 3

# Facebook's async insights jobs fail intermittently server-side ("Job Failed" with
# 0% progress and no detail) and the Graph API throttles calls with transient
# errors on any call: submit, status poll, or paging a finished job's results.
# Neither is a real fault in the request, so a failed chunk is resubmitted and
# throttled calls are retried with exponential backoff before giving up.
DEFAULT_MAX_JOB_ATTEMPTS = 3
API_RETRY_MAX_TRIES = 5
API_RETRY_BASE_SECONDS = 30
# Graph API error codes that indicate throttling or a transient outage.
# 1/2 unknown/service unavailable, 4 app-level request limit, 17 user-level
# request limit, 32 page-level request limit, 613 custom rate limit,
# 80000/80004 Marketing API insights call load.
TRANSIENT_ERROR_CODES = frozenset({1, 2, 4, 17, 32, 613, 80000, 80004})


class AdsInsightStream(Stream):
    name = "adsinsights"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "date_start"

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the stream."""
        self._report_definition = kwargs.pop("report_definition")
        kwargs["name"] = f"{self.name}_{self._report_definition['name']}"
        super().__init__(*args, **kwargs)

    @property
    def primary_keys(self) -> t.Sequence[str]:
        return ["date_start", "account_id", "ad_id"] + self._report_definition["breakdowns"]

    @primary_keys.setter
    def primary_keys(self, new_value: t.Sequence[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @staticmethod
    def _get_datatype(field: str) -> th.JSONTypeHelper | None:
        d_type = AdsInsights._field_types[field]  # noqa: SLF001
        if d_type == "string":
            return th.StringType()
        if d_type in ("int", "unsigned int"):
            return th.IntegerType()
        if d_type.startswith("list"):
            sub_props: list[th.Property]
            if "AdsActionStats" in d_type:
                sub_props = [
                    th.Property(field.replace("field_", ""), th.StringType())
                    for field in list(AdsActionStats.Field.__dict__)
                    if field not in EXCLUDED_FIELDS
                ]
                return th.ArrayType(th.ObjectType(*sub_props))
            if "AdsHistogramStats" in d_type:
                sub_props = []
                for f in list(AdsHistogramStats.Field.__dict__):
                    if f not in EXCLUDED_FIELDS:
                        clean_field = f.replace("field_", "")
                        if AdsHistogramStats._field_types[clean_field] == "string":  # noqa: SLF001
                            sub_props.append(th.Property(clean_field, th.StringType()))
                        else:
                            sub_props.append(
                                th.Property(
                                    clean_field,
                                    th.ArrayType(th.IntegerType()),
                                ),
                            )
                return th.ArrayType(th.ObjectType(*sub_props))
            return th.ArrayType(th.ObjectType())
        msg = f"Type not found for field: {field}"
        raise RuntimeError(msg)

    @property
    @lru_cache  # noqa: B019
    def schema(self) -> dict:
        properties: list[th.Property] = []
        columns = list(AdsInsights.Field.__dict__)[1:]
        for field in columns:
            if field in EXCLUDED_FIELDS:
                continue
            if data_type := self._get_datatype(field):
                properties.append(th.Property(field, data_type))

        properties.extend(
            [
                th.Property(breakdown, th.StringType())
                for breakdown in self._report_definition["breakdowns"]
            ],
        )

        return th.PropertiesList(*properties).to_dict()

    def _initialize_client(self) -> None:
        FacebookAdsApi.init(
            access_token=self.config["access_token"],
            timeout=300,
            api_version=self.config["api_version"],
        )
        fb_user.User(fbid="me")

        account_id = self.config["account_id"]
        self.account = AdAccount(f"act_{account_id}").api_get()
        if not self.account:
            msg = f"Couldn't find account with id {account_id}"
            raise RuntimeError(msg)

    def _build_params(
        self,
        columns: list[str],
        time_increment: int,
        since: pendulum.Date,
        until: pendulum.Date,
    ) -> dict:
        return {
            "level": self._report_definition["level"],
            "action_breakdowns": self._report_definition["action_breakdowns"],
            "action_report_time": self._report_definition["action_report_time"],
            "breakdowns": self._report_definition["breakdowns"],
            "fields": columns,
            "time_increment": time_increment,
            "limit": 100,
            "action_attribution_windows": [
                self._report_definition["action_attribution_windows_view"],
                self._report_definition["action_attribution_windows_click"],
            ],
            "time_range": {
                "since": since.to_date_string(),
                "until": until.to_date_string(),
            },
        }

    @staticmethod
    def _is_transient_error(err: FacebookRequestError) -> bool:
        """Whether a Graph API error is throttling/outage rather than a bad request."""
        if err.api_transient_error():
            return True
        return err.api_error_code() in TRANSIENT_ERROR_CODES

    def _call_with_backoff(self, what: str, fn: t.Callable[[], T]) -> T:
        """Run one Graph API call, retrying transient errors with exponential backoff."""

        def _log_retry(details: backoff.types.Details) -> None:
            # backoff's Details TypedDict omits the key that on_exception handlers get.
            err = details["exception"]  # type: ignore[typeddict-item]
            self.logger.warning(
                "Transient Facebook API error during %s (code %s, subcode %s): %s. "
                "Retrying in %.0fs (attempt %s/%s).",
                what,
                err.api_error_code(),
                err.api_error_subcode(),
                err.api_error_message(),
                details["wait"],
                details["tries"],
                API_RETRY_MAX_TRIES,
            )

        retrying = backoff.on_exception(
            backoff.expo,
            FacebookRequestError,
            max_tries=API_RETRY_MAX_TRIES,
            factor=API_RETRY_BASE_SECONDS,
            jitter=None,
            giveup=lambda err: not self._is_transient_error(err),
            on_backoff=_log_retry,
        )
        return retrying(fn)()

    def _submit_job(self, params: dict, attempt: int = 1) -> dict:
        """Start one async insights job for `params` and return its in-flight entry."""
        time_range = params["time_range"]
        job = self._call_with_backoff(
            f"submit of {time_range['since']} - {time_range['until']}",
            lambda: self.account.get_insights(params=params, is_async=True),
        )
        return {"job": job, "params": params, "started": time.time(), "attempt": attempt}

    def _fetch_results(self, entry: dict) -> list[dict]:
        """Page through a completed job's rows, retrying transient errors from page one.

        Facebook returns the rows of an async report through a paginated cursor, and
        each page is its own Graph API call that can fail transiently just like a
        submit or a status poll. A retry restarts the cursor from the first page and
        may re-yield rows already emitted; that is safe because the loader upserts on
        the stream's primary key, and it is far cheaper than failing the whole sync.
        """
        time_range = entry["params"]["time_range"]
        return self._call_with_backoff(
            f"result fetch of {time_range['since']} - {time_range['until']}",
            lambda: [obj.export_all_data() for obj in entry["job"].get_result()],
        )

    def _resubmit_or_raise(self, entry: dict, reason: str) -> str:
        """Replace a failed/stuck job with a fresh one, or raise once attempts run out."""
        time_range = entry["params"]["time_range"]
        attempt = entry["attempt"]
        if attempt >= entry["max_attempts"]:
            msg = (
                f"Insights job {entry['job']['id']} for {time_range['since']} - "
                f"{time_range['until']} {reason} on attempt {attempt}/"
                f"{entry['max_attempts']}; giving up. This is usually an intermittent "
                "Facebook-side error and may resolve on a later run. Deselecting "
                "unnecessary fields can also improve reliability. "
                f"Last job: {dict(entry['job'])}"
            )
            raise RuntimeError(msg)
        self.logger.warning(
            "Insights job %s for %s - %s %s (attempt %s/%s). Resubmitting.",
            entry["job"]["id"],
            time_range["since"],
            time_range["until"],
            reason,
            attempt,
            entry["max_attempts"],
        )
        fresh = self._submit_job(entry["params"], attempt=attempt + 1)
        fresh["max_attempts"] = entry["max_attempts"]
        entry.update(fresh)
        return "Job Resubmitted"

    def _check_job_status(self, entry: dict) -> str:
        """Refresh one in-flight job; resubmit it if Facebook failed or stalled it."""
        time_range = entry["params"]["time_range"]
        entry["job"] = self._call_with_backoff(
            f"status poll of {time_range['since']} - {time_range['until']}",
            entry["job"].api_get,
        )
        job = entry["job"]
        status = job[AdReportRun.Field.async_status]
        percent_complete = job[AdReportRun.Field.async_percent_completion]
        self.logger.info(
            "%s for %s - %s. %s%% done.",
            status,
            time_range["since"],
            time_range["until"],
            percent_complete,
        )
        if status in ("Job Failed", "Job Skipped"):
            detail = (
                f" ({job.get('error_code')}/{job.get('error_subcode')}: {job.get('error_message')})"
            )
            return self._resubmit_or_raise(entry, f"reported '{status}'{detail}")
        duration = time.time() - entry["started"]
        if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
            return self._resubmit_or_raise(
                entry,
                f"did not start after {INSIGHTS_MAX_WAIT_TO_START_SECONDS} seconds",
            )
        if duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS:
            msg = (
                f"Insights job {job['id']} did not complete after "
                f"{INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS // 60} minutes. "
                "This is an intermittent error and may resolve itself on subsequent "
                "queries to the Facebook API. You should deselect fields from the "
                "schema that are not necessary, as that may help improve the "
                "reliability of the Facebook API."
            )
            raise RuntimeError(msg)
        return status

    def _get_selected_columns(self) -> list[str]:
        columns = [
            keys[1] for keys, data in self.metadata.items() if data.selected and len(keys) > 0
        ]
        if not columns and self.name == "adsinsights_default":
            columns = list(self.schema["properties"])
        return columns

    def _get_start_date(
        self,
        context: Context | None,
    ) -> pendulum.Date:
        lookback_window = self._report_definition["lookback_window"]

        config_start_date = pendulum.parse(self.config["start_date"]).date()  # type: ignore[union-attr]
        incremental_start_date = pendulum.parse(  # type: ignore[union-attr]
            self.get_starting_replication_key_value(context),  # type: ignore[arg-type]
        ).date()
        lookback_start_date = incremental_start_date.subtract(days=lookback_window)

        # Don't use lookback if this is the first sync. Just start where the user requested.
        if config_start_date >= incremental_start_date:
            report_start = config_start_date
            self.logger.info("Using configured start_date as report start filter.")
        else:
            self.logger.info(
                "Incremental sync, applying lookback '%s' to the "
                "bookmark start_date '%s'. Syncing "
                "reports starting on '%s'.",
                lookback_window,
                incremental_start_date,
                lookback_start_date,
            )
            report_start = lookback_start_date

        # Facebook store metrics maximum of 37 months old. Any time range that
        # older that 37 months from current date would result in 400 Bad request
        # HTTP response.
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
        today = pendulum.today().date()
        oldest_allowed_start_date = today.subtract(months=37)
        if report_start < oldest_allowed_start_date:
            report_start = oldest_allowed_start_date
            self.logger.info(
                "Report start date '%s' is older than 37 months. "
                "Using oldest allowed start date '%s' instead.",
                report_start,
                oldest_allowed_start_date,
            )
        return report_start

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        self._initialize_client()

        time_increment = self._report_definition["time_increment_days"]

        sync_end_date = pendulum.parse(  # type: ignore[union-attr]
            self.config.get("end_date", pendulum.today().to_date_string()),
        ).date()

        report_start = self._get_start_date(context)
        columns = self._get_selected_columns()

        chunk_days = self._report_definition.get(
            "report_chunk_days",
            DEFAULT_REPORT_CHUNK_DAYS,
        )
        max_concurrent = max(
            1,
            self._report_definition.get(
                "max_concurrent_jobs",
                DEFAULT_MAX_CONCURRENT_JOBS,
            ),
        )
        max_attempts = max(
            1,
            self._report_definition.get(
                "max_job_attempts",
                DEFAULT_MAX_JOB_ATTEMPTS,
            ),
        )

        # Build the request windows. Each async job pulls `chunk_days` at once but is
        # still bucketed by `time_increment` days server-side, so daily granularity is
        # preserved while collapsing ~chunk_days one-day jobs into a single job.
        pending: deque[tuple[pendulum.Date, pendulum.Date]] = deque()
        window_start = report_start
        while window_start <= sync_end_date:
            pending.append((window_start, window_start.add(days=chunk_days)))
            window_start = window_start.add(days=chunk_days)

        # Bounded-concurrency pipeline: keep up to `max_concurrent` async jobs in
        # flight on Facebook's side (it computes them in parallel) and poll them
        # together, overlapping the otherwise-dead wait time. Single-threaded, so no
        # SDK thread-safety concerns. Results may arrive out of date order, which is
        # fine: the stream is unsorted, so state finalises to the max date_start.
        in_flight: list[dict] = []

        def submit_jobs() -> None:
            while len(in_flight) < max_concurrent and pending:
                since, until = pending.popleft()
                params = self._build_params(columns, time_increment, since, until)
                entry = self._submit_job(params)
                entry["max_attempts"] = max_attempts
                in_flight.append(entry)

        submit_jobs()
        while in_flight:
            made_progress = False
            for entry in list(in_flight):
                if self._check_job_status(entry) == "Job Completed":
                    in_flight.remove(entry)
                    yield from self._fetch_results(entry)
                    made_progress = True
            submit_jobs()  # top up freed slots
            if in_flight and not made_progress:
                time.sleep(SLEEP_TIME_INCREMENT)
