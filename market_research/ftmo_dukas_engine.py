"""Separate long/short hourly basket replays with explicit broker cost scenarios."""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from zoneinfo import ZoneInfo

from .ftmo_dukas_data import INSTRUMENTS, UTC, stamp

SERVER = ZoneInfo('Europe/Helsinki')  # GMT+2/+3; historical server offsets are a documented assumption.
PRAGUE = ZoneInfo('Europe/Prague')


@dataclass(frozen=True)
class Rule:
    name: str
    median: bool = False
    momentum: int = 0
    cap: int | None = None
    delay: int = 0
    volatility: bool = False
    cost_gate: bool = False


RULES = (
    Rule('baseline'), Rule('median', median=True),
    Rule('median_momentum3', median=True, momentum=3),
    Rule('median_momentum6', median=True, momentum=6),
    Rule('median_cap3', median=True, cap=3),
    Rule('median_momentum3_cap3', median=True, momentum=3, cap=3),
    Rule('median_delay3', median=True, delay=3),
    Rule('median_volatility', median=True, volatility=True),
    Rule('median_cost_gate', median=True, cost_gate=True),
)


@dataclass(frozen=True)
class Costs:
    name: str
    spread_factor: float = 1.
    percent_roundtrip: bool = False
    adverse_swap_factor: float = 1.
    positive_swaps: bool = True
    daily_swaps: bool = False


COSTS = (Costs('reference_percentage_per_side'),
         Costs('percentage_roundtrip_sensitivity', percent_roundtrip=True),
         Costs('spread_and_swap_stress', spread_factor=2., adverse_swap_factor=1.5, positive_swaps=False),
         Costs('daily_rollover_sensitivity', daily_swaps=True))


def adjusted_quotes(row: dict, symbol: str, costs: Costs):
    floor = INSTRUMENTS[symbol][2]*costs.spread_factor
    extra=max(0.,floor-(row['ask']['o']-row['bid']['o']))/2
    return ({k:row['bid'][k]-extra for k in ('o','h','l','c')},
            {k:row['ask'][k]+extra for k in ('o','h','l','c')})


def commission(spec: dict, lots: float, price: float, costs: Costs) -> float:
    rate=float(spec['commission'])
    if spec['commissionType'] == 'flat_USD':
        # FTMO US explicitly documents $5 per lot ROUND TRIP for spot FX.
        return lots*rate/2
    if spec['commissionType'] != 'percent' or spec['profitCurrency'] != 'USD':
        raise ValueError('UNSUPPORTED_COMMISSION_CURRENCY_OR_TYPE')
    # The public table does not say per-side versus round-trip for percentages.
    # Both interpretations remain visible; reference uses the higher cost.
    return lots*spec['contractSize']*price*rate/100/(2 if costs.percent_roundtrip else 1)


def swap_weight(symbol: str, local_date, costs: Costs) -> int:
    if costs.daily_swaps or symbol == 'BTCUSD.sim':
        return 1
    weekday=local_date.weekday()
    if weekday >= 5:
        return 0
    triple=4 if symbol in ('US500.sim','US30.sim','US100.sim') else 2
    return 3 if weekday == triple else 1


def swap_quote(spec: dict, side: str, lots: float, price: float, days: int, costs: Costs) -> float:
    if spec['swapType'] != 'percentage':
        raise ValueError('ANNUAL_PERCENT_SWAP_REQUIRED')
    rate=float(spec['swapLong' if side == 'long' else 'swapShort'])
    rate=rate*costs.adverse_swap_factor if rate<0 else rate if costs.positive_swaps else 0.
    # MT5 interest/current uses a 360-day banking year, NOT the raw percentage each night.
    return lots*spec['contractSize']*price*rate/100/360*days


def bar_tradeable(start: datetime, spec: dict) -> bool:
    local=start.astimezone(SERVER); minute=local.weekday()*1440+local.hour*60+local.minute
    # Current weekly hours are projected backwards; no historical holiday/maintenance claim.
    return any(a <= minute and minute+60 <= b for a,b in spec['weekly_minutes'])


class Conversion:
    def __init__(self, currency: str, rows: list[dict]):
        self.currency=currency;self.rows=rows;self.times=[stamp(r['start']) for r in rows]

    def rate(self, at: datetime, field='o') -> tuple[float,float]:
        if self.currency == 'USD':
            return 1.,1.
        index=bisect_right(self.times,at)-1
        if index<0 or (at-self.times[index]).total_seconds()>72*3600:
            raise ValueError('USD_CONVERSION_QUOTE_UNAVAILABLE')
        row=self.rows[index]
        # A prior hour's close is known; an uncompleted hour's close is never used at its open.
        selected=field if self.times[index] == at else 'c'
        return row['bid'][selected],row['ask'][selected]

    def usd(self, amount: float, at: datetime, field='o') -> float:
        bid,ask=self.rate(at,field)
        return amount/(ask if amount>=0 else bid)


def _metrics(curve, baskets, entries, sessions, first: datetime, last: datetime):
    points=[r for r in curve if first < stamp(r['at']) <= last]
    if not points:
        raise ValueError('METRIC_PERIOD_HAS_NO_OBSERVATIONS')
    # Every replay starts flat; a diagnostic slice begins with its actual equity, including held positions.
    start_equity=points[0]['start_equity'];peak=start_equity;dd=0.
    for r in points:
        dd=max(dd,peak-r['low']);peak=max(peak,r['close'])
    closed=[r for r in baskets if first < stamp(r['accounted_at']) <= last]
    legs=[r for r in entries if first < stamp(r['accounted_at']) <= last]
    daily=[r for r in sessions if first <= stamp(r['start']) < last]
    exposed=[r for r in daily if r['exposed']]
    wins=sum(r['pnl']>0 for r in legs);losses=sum(r['pnl']<0 for r in legs)
    bw=sum(r['pnl']>0 for r in closed);bl=sum(r['pnl']<0 for r in closed)
    sw=sum(r['pnl']>0 for r in exposed);sl=sum(r['pnl']<0 for r in exposed)
    return {'net_equity_pnl':points[-1]['close']-start_equity,'max_equity_drawdown':dd,
        'closed_baskets':len(closed),'basket_wins':bw,'basket_losses':bl,'basket_win_rate':bw/len(closed)if closed else None,
        'closed_entries':len(legs),'entry_wins':wins,'entry_losses':losses,'entry_win_rate':wins/len(legs)if legs else None,
        'exposed_sessions':len(exposed),'winning_sessions':sw,'losing_sessions':sl,
        'session_win_rate':sw/len(exposed)if exposed else None,
        'closed_basket_net_pnl':sum(r['pnl']for r in closed)}


def replay(symbol: str, side: str, rows: list[dict], forecasts: list[dict], spec: dict,
           conversion: Conversion, first: datetime, last: datetime, split: datetime,
           rule: Rule=RULES[0], costs: Costs=COSTS[0], ordering='low_first', initial_balance=100_000.):
    if side not in ('long','short') or ordering not in ('low_first','high_first'):
        raise ValueError('SIDE_OR_PATH_INVALID')
    lots=.01; units=lots*float(spec['contractSize']);direction=1 if side=='long'else -1
    distance=INSTRUMENTS[symbol][1];digits=int(spec['digits']);tick=10**-digits
    by_time={stamp(r['start']):r for r in rows};history=[];features={}
    for row in rows:
        t=stamp(row['start']);features[t]=list(history[-70:]);history.append(row)
    predictions={};origins={}
    for forecast in forecasts:
        if forecast.get('status')!='completed':continue
        origin=stamp(forecast['origin']);pred=forecast['predictions']
        favorable=pred[-1]['p50']>=pred[0]['p50'] if side=='long' else pred[-1]['p50']<=pred[0]['p50']
        for p in pred:
            end=stamp(p['timestamp']);start=end-timedelta(hours=1)
            if start in predictions:raise ValueError('OVERLAPPING_DAILY_FORECAST')
            predictions[start]=(p,stamp(forecast['earliest_actionable_at']),favorable,origin)
        origins[origin]=forecast
    basket=[];trades=[];baskets=[];curve=[];sessions=[];cash=initial_balance
    paid_commission=accrued_swap=0.;max_lots=0.;max_margin=0.;max_daily_loss=0.
    blocked_margin=blocked_volume=0;first_daily_breach=first_total_breach=first_margin_breach=None
    last_bid=last_ask=None;last_quote_at=None;server_day=None;daily_balance=cash
    session_start=first;session_equity=cash;session_exposed=False

    def equity(bid,ask,at,field='o'):
        value=cash
        for leg in basket:
            exit_price=bid if side=='long'else ask
            value+=conversion.usd(direction*units*(exit_price-leg['entry']),at,field)-commission(spec,lots,exit_price,costs)
        return value

    def target():
        if not basket:return None
        raw=sum(r['entry']for r in basket)/len(basket)+direction*distance
        return (math.ceil(raw/tick-1e-8)if side=='long'else math.floor(raw/tick+1e-8))*tick

    def close(price,at,conversion_at):
        nonlocal cash,paid_commission
        net=0.
        for leg in basket:
            fee=commission(spec,lots,price,costs);gross=conversion.usd(direction*units*(price-leg['entry']),conversion_at)
            pnl=gross-leg['commission']-fee+leg['swap'];cash+=gross-fee;paid_commission+=fee;net+=pnl
            trades.append({**leg,'exit_at':at.isoformat(),'accounted_at':(conversion_at+timedelta(hours=1)).isoformat(),'exit':price,'pnl':pnl,'exit_commission':fee})
        baskets.append({'entry_at':basket[0]['entry_at'],'exit_at':at.isoformat(),'accounted_at':(conversion_at+timedelta(hours=1)).isoformat(),'entries':len(basket),'pnl':net})
        basket.clear()

    def buy(price,at,conversion_at,bid,ask):
        nonlocal cash,paid_commission,max_lots,max_margin,blocked_margin,blocked_volume
        volume=(len(basket)+1)*lots
        if lots>float(spec['maxTradeVolume']) or volume>float(spec['maxTotalVolume'])+1e-8:
            blocked_volume+=1;return False
        notional=abs(conversion.usd(volume*float(spec['contractSize'])*price,conversion_at))
        margin=notional/float(spec['leverageStandard'])
        if margin>equity(bid,ask,conversion_at)-commission(spec,lots,price,costs):
            blocked_margin+=1;return False
        fee=commission(spec,lots,price,costs);cash-=fee;paid_commission+=fee
        basket.append({'entry_at':at.isoformat(),'entry':price,'lots':lots,'commission':fee,'swap':0.})
        max_lots=max(max_lots,volume);max_margin=max(max_margin,margin);return True

    at=first;last_equity=cash
    while at<last:
        row=by_time.get(at);end=min(at+timedelta(hours=1),last)
        if row:
            bid,ask=adjusted_quotes(row,symbol,costs)
            bid={k:math.floor(v/tick+1e-8)*tick for k,v in bid.items()}
            ask={k:math.ceil(v/tick-1e-8)*tick for k,v in ask.items()}
            last_bid,last_ask=bid['o'],ask['o'];last_quote_at=at
        elif last_bid is None:
            # The first timeline hour can be closed. Only genuinely earlier prices may mark a carried basket.
            previous=[r for r in rows if stamp(r['end'])<=at]
            if not previous:raise ValueError('PRE_SESSION_QUOTE_REQUIRED')
            saved=previous[-1];last_bid,last_ask=saved['bid']['c'],saved['ask']['c'];last_quote_at=stamp(saved['start'])
        quote_at=at if row else last_quote_at
        local=at.astimezone(SERVER)
        risk_day=at.astimezone(PRAGUE).date()
        if risk_day!=server_day:server_day=risk_day;daily_balance=cash
        beginning=equity(last_bid,last_ask,quote_at)
        if at>session_start and at.hour==18:
            sessions.append({'start':session_start.isoformat(),'end':at.isoformat(),'pnl':beginning-session_equity,'exposed':session_exposed})
            session_start=at;session_equity=beginning;session_exposed=bool(basket)
        if local.hour==0:
            days=swap_weight(symbol,(local-timedelta(days=1)).date(),costs)
            for leg in basket:
                value=conversion.usd(swap_quote(spec,side,lots,(last_bid+last_ask)/2,days,costs),quote_at)
                leg['swap']+=value;cash+=value;accrued_swap+=value
        low_equity=equity(last_bid,last_ask,quote_at)
        sold=bought=False
        if row and not bar_tradeable(at,spec):
            low_equity=min(low_equity,equity(bid['l'],ask['h'],at))
        if row and bar_tradeable(at,spec):
            info=predictions.get(at);allowed=False;level=None
            prior=features[at]
            if info:
                p,available,favorable,origin=info
                allowed=available<=at and (not rule.median or favorable) and at>=origin+timedelta(hours=rule.delay)
                if rule.cap is not None and len(basket)>=rule.cap:allowed=False
                if rule.momentum and not basket:
                    history=[r for r in prior if stamp(r['end'])<=at]
                    recent={stamp(r['end']):(r['bid']['c']+r['ask']['c'])/2 for r in history}
                    now,past=recent.get(at),recent.get(at-timedelta(hours=rule.momentum))
                    allowed=allowed and now is not None and past is not None and direction*(now-past)>=0
                if rule.volatility:
                    true_ranges=[];previous_close=None
                    for h in prior:
                        hi=(h['bid']['h']+h['ask']['h'])/2;lo=(h['bid']['l']+h['ask']['l'])/2
                        true_ranges.append(max(hi-lo,abs(hi-previous_close),abs(lo-previous_close))if previous_close is not None else hi-lo)
                        previous_close=(h['bid']['c']+h['ask']['c'])/2
                    import statistics
                    allowed=allowed and len(true_ranges)>=56 and sum(true_ranges[-14:])/14<=1.25*statistics.median(true_ranges[-56:])
                level=p['p01' if side=='long'else 'p99']
                level=(math.ceil(level/tick-1e-8)-1 if side=='long'else math.floor(level/tick+1e-8)+1)*tick
                if basket:level=min(level,basket[-1]['entry']-distance)if side=='long'else max(level,basket[-1]['entry']+distance)
                if rule.cost_gate:
                    price=ask['o']if side=='long'else bid['o'];gross=conversion.usd(units*distance,at)
                    fees=commission(spec,lots,price,costs)+commission(spec,lots,price+direction*distance,costs)
                    swap=conversion.usd(swap_quote(spec,side,lots,price,1,costs),at)
                    allowed=allowed and gross>fees+max(0.,-swap)
            keys=('o','l','h','c')if ordering=='low_first'else ('o','h','l','c')
            entry_open=ask['o']if side=='long'else bid['o'];exit_open=bid['o']if side=='long'else ask['o'];active=target()
            if active is not None and direction*(exit_open-active)>=-1e-10:
                close(exit_open,at,at);sold=True
            elif allowed and level is not None and direction*(entry_open-level)<0:
                bought=buy(entry_open,at,at,bid['o'],ask['o'])
            low_equity=min(low_equity,equity(bid['o'],ask['o'],at))
            for left,right in zip(keys,keys[1:]):
                previous_exit=bid[left]if side=='long'else ask[left];exit_price=bid[right]if side=='long'else ask[right]
                previous_entry=ask[left]if side=='long'else bid[left];entry_price=ask[right]if side=='long'else bid[right];active=target()
                if not sold and active is not None and direction*(previous_exit-active)<0<=direction*(exit_price-active):
                    close(active,end,at);sold=True
                elif not sold and not bought and allowed and level is not None and direction*(previous_entry-level)>=0>direction*(entry_price-level):
                    # At most one addition per observed hourly bar. Crossing times are unknown.
                    spread=ask['o']-bid['o']
                    bought=buy(level,end,at,level-spread if side=='long'else level,level if side=='long'else level+spread)
                low_equity=min(low_equity,equity(bid[right],ask[right],at))
        if row:
            last_bid,last_ask=bid['c'],ask['c'];last_quote_at=at
            last_equity=equity(last_bid,last_ask,at,'c')
        else:last_equity=equity(last_bid,last_ask,quote_at)
        session_exposed=session_exposed or bool(basket) or bought or sold
        daily_loss=max(0.,daily_balance-low_equity);max_daily_loss=max(max_daily_loss,daily_loss)
        if daily_loss>5000 and first_daily_breach is None:first_daily_breach=end.isoformat()
        if low_equity<initial_balance-10000 and first_total_breach is None:first_total_breach=end.isoformat()
        volume=len(basket)*lots
        margin=abs(conversion.usd(volume*float(spec['contractSize'])*(last_bid+last_ask)/2,quote_at))/float(spec['leverageStandard'])
        max_margin=max(max_margin,margin)
        if basket and margin>low_equity and first_margin_breach is None:first_margin_breach=end.isoformat()
        curve.append({'at':end.isoformat(),'start_equity':beginning,'low':min(low_equity,last_equity),'close':last_equity})
        at=end
    sessions.append({'start':session_start.isoformat(),'end':last.isoformat(),'pnl':last_equity-session_equity,'exposed':session_exposed})
    full=_metrics(curve,baskets,trades,sessions,first,last)
    development=_metrics(curve,baskets,trades,sessions,first,min(split,last))if split>first else None
    later=_metrics(curve,baskets,trades,sessions,max(split,first),last)if split<last else None
    return {'rule':rule.name,'costs':costs.name,'ordering':ordering,'full':full,'development':development,'later_carried_diagnostic':later,
        'open_entries':len(basket),'open_lots':len(basket)*lots,'oldest_open_entry':basket[0]['entry_at']if basket else None,
        'open_net_pnl':full['net_equity_pnl']-full['closed_basket_net_pnl'],
        'commission_paid':paid_commission,'swap_accrued':accrued_swap,'max_open_lots':max_lots,'max_margin_usd':max_margin,
        'max_ftmo_midnight_balance_daily_loss':max_daily_loss,'first_daily_limit_breach':first_daily_breach,
        'first_total_limit_breach':first_total_breach,'first_margin_breach':first_margin_breach,
        'margin_blocked_additions':blocked_margin,'volume_blocked_additions':blocked_volume,
        'trades':trades,'baskets':baskets,'session_equity_changes':sessions,'open_basket':basket}
