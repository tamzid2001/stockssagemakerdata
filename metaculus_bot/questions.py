from __future__ import annotations

import copy
import math

from .api import PERMISSIONS, date, utcnow


def unpack(post: dict) -> list:
    """Use the official SDK's scale/date/group/conditional semantics."""
    if post.get("user_permission") not in PERMISSIONS or post.get("status") != "open":
        return []
    from forecasting_tools.helpers.metaculus_client import MetaculusClient
    if not any(post.get(k) for k in ["question", "group_of_questions", "conditional"]):
        return []
    parsed = MetaculusClient()._post_json_to_questions_while_handling_groups(copy.deepcopy(post), "unpack_subquestions")
    from forecasting_tools.data_models.questions import ConditionalQuestion
    result = []
    for q in parsed:
        if isinstance(q, ConditionalQuestion):
            # Do not forecast the condition or unconditional child incidentally.
            result.extend([q.question_yes, q.question_no])
        else:
            result.append(q)
    return [q for q in result if is_open(q)]


def is_open(question, now=None) -> bool:
    now = now or utcnow()
    return (str(question.state.value) == "open"
            and (question.open_time is None or question.open_time <= now)
            and (question.close_time is None or question.close_time > now))


def already_submitted(question) -> bool:
    # All new questions get one forecast. Existing authoritative history also
    # reconciles a response lost after the server accepted a submission.
    return bool(question.already_forecasted or question.timestamp_of_my_last_forecast)


def context(question) -> dict:
    # Never pass arbitrary API JSON (community/other bot forecasts) into the LLM.
    values = {"title": question.question_text, "description": question.background_info,
              "resolution_criteria": question.resolution_criteria, "fine_print": question.fine_print,
              "type": question.question_type, "unit": question.unit_of_measure,
              "close_time": question.close_time.isoformat() if question.close_time else None}
    if question.question_type == "multiple_choice":
        values["options"] = question.options
    elif question.question_type in {"numeric", "discrete", "date"}:
        values.update({"lower_bound": str(question.lower_bound), "upper_bound": str(question.upper_bound),
                       "open_lower_bound": question.open_lower_bound, "open_upper_bound": question.open_upper_bound,
                       "zero_point": question.zero_point})
    return values


def payload(question, answer: dict) -> dict:
    kind = question.question_type
    base = {"question": question.id_of_question, "source": "api", "probability_yes": None,
            "probability_yes_per_category": None, "continuous_cdf": None}
    if kind == "binary":
        value = float(answer["probability_yes"])
        if not math.isfinite(value) or not 0.001 <= value <= 0.999:
            raise ValueError("INVALID_BINARY_PROBABILITY")
        base["probability_yes"] = value
    elif kind == "multiple_choice":
        probs = answer["probabilities"]
        if set(probs) != set(question.options):
            raise ValueError("EXACT_OPTIONS_REQUIRED")
        values = {k: float(v) for k, v in probs.items()}
        if any(not math.isfinite(v) or not 0.001 <= v <= 0.999 for v in values.values()):
            raise ValueError("INVALID_CHOICE_PROBABILITY")
        total = sum(values.values())
        if abs(total - 1) > 0.01:
            raise ValueError("CHOICE_PROBABILITIES_MUST_SUM_TO_ONE")
        base["probability_yes_per_category"] = {k: v / total for k, v in values.items()}
    elif kind in {"numeric", "discrete", "date"}:
        from forecasting_tools.data_models.numeric_report import NumericDistribution, Percentile
        points = answer["percentiles"]
        required = [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99]
        if [p["percentile"] for p in points] != required:
            raise ValueError("SEVEN_ORDERED_PERCENTILES_REQUIRED")
        percentiles = []
        for p in points:
            value = date(p["value"]).timestamp() if kind == "date" else float(p["value"])
            if not math.isfinite(value):
                raise ValueError("FINITE_QUANTILES_REQUIRED")
            percentiles.append(Percentile(percentile=p["percentile"], value=value))
        distribution = NumericDistribution.from_question(percentiles, question)
        cdf = [p.percentile for p in distribution.get_cdf()]
        if len(cdf) != question.cdf_size or any(not math.isfinite(x) or not 0 <= x <= 1 for x in cdf):
            raise ValueError("INVALID_CDF")
        if any(b < a for a, b in zip(cdf, cdf[1:])):
            raise ValueError("NONMONOTONE_CDF")
        base["continuous_cdf"] = cdf
    else:
        raise ValueError("QUESTION_TYPE_UNSUPPORTED")
    return base
