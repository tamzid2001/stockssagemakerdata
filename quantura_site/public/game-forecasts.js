/* Public saved snapshots; viewing never queues inference or trading. */
(() => {
  'use strict';
  const page=document.getElementById('game-forecast-page');
  const bands=['0.01','0.25','0.5','0.75','0.9','0.99'];let sequence=0,viewed=null,saveBusy=false;
  const savedFor=new Set();
  async function saveViewed(){
    const user=window.firebase?.auth?.().currentUser,entry=viewed;
    if(!entry||entry.saved||!user||user.isAnonymous||saveBusy)return;
    const key=user.uid+':'+entry.item.id+':'+entry.item.generated_at;if(savedFor.has(key))return;
    saveBusy=true;
    try{
      const token=await (window.QuanturaAuth?.getToken(user) ?? user.getIdToken());
      const response=await fetch(`/api/screener/games/${entry.item.id}/save`,{method:'POST',headers:{Authorization:`Bearer ${token}`},credentials:'same-origin',signal:AbortSignal.timeout(15000)});
      if(!response.ok)throw Error();const result=await response.json();savedFor.add(key);
      if(viewed===entry && window.firebase?.auth?.().currentUser?.uid===user.uid){const status=el('p','Saved to your profile’s Requests.','small');status.setAttribute('role','status');page.append(status);}
      document.dispatchEvent(new CustomEvent('quantura:request-saved',{detail:{requestId:result.request_id}}));
    }catch{if(viewed===entry){const status=el('p','Forecast loaded. Saving to Requests failed.','small'),retry=el('button','Retry save','cta secondary');retry.type='button';retry.addEventListener('click',saveViewed);status.append(retry);page.append(status);}}finally{saveBusy=false;if(viewed!==entry)saveViewed();}
  }
  const time=v=>new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZone:'America/New_York',timeZoneName:'short'}).format(new Date(v));
  const el=(tag,text='',cls)=>{const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;};
  const percent=v=>Number.isFinite(v)?`${(100*v).toFixed(1)}%`:'—',label=q=>'P'+String(Math.round(Number(q)*100)).padStart(2,'0');
  const href=id=>`/forecasting?panel=forecast&gameForecastId=${encodeURIComponent(id)}`;
  function leaveSavedView(){
    sequence++;viewed=null;document.body.classList.remove('game-forecast-view');
    if(page){page.hidden=true;page.replaceChildren();page.removeAttribute('aria-busy');}
    const url=new URL(window.location.href);url.searchParams.delete('gameForecastId');url.searchParams.delete('userGameForecastId');
    window.history.replaceState(window.history.state,'',url.pathname+url.search+url.hash);
  }
  window.addEventListener('quantura:market-selected',event=>{if(event.detail?.resource&&event.detail.intent==='forecast')leaveSavedView();});
  window.addEventListener('quantura:panel-changed',event=>{if(event.detail?.panel!=='forecast'&&document.body.classList.contains('game-forecast-view'))leaveSavedView();});
  document.addEventListener('click',event=>{if(event.target.closest?.('[data-panel-target="forecast"],[data-q-upload],#q-upload-csv')&&document.body.classList.contains('game-forecast-view'))leaveSavedView();},true);
  function provider(game){const n=el('div',game.provider==='kalshi'?'Kalshi':'Polymarket US','game-provider small');if(window.QuanturaLogos)n.insertAdjacentHTML('afterbegin',window.QuanturaLogos.markup(game));return n;}
  const normalize=value=>String(value||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
  const teamCodes={
    mlb:'ARI ATL BAL BOS CHC CHW CIN CLE COL DET HOU KCR LAA LAD MIA MIL MIN NYM NYY ATH PHI PIT SDP SEA SFG STL TBR TEX TOR WSN'.split(' '),
    nfl:'ARI ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC LAC LAR LV MIA MIN NE NO NYG NYJ PHI PIT SEA SF TB TEN WAS'.split(' '),
    nba:'ATL BOS BKN CHA CHI CLE DAL DEN DET GSW HOU IND LAC LAL MEM MIA MIL MIN NOP NYK OKC ORL PHI PHX POR SAC SAS TOR UTA WAS'.split(' '),
    nhl:'ANA BOS BUF CAR CBJ CGY CHI COL DAL DET EDM FLA LAK MIN MTL NJD NSH NYI NYR OTT PHI PIT SEA SJS STL TBL TOR UTA VAN VGK WPG WSH'.split(' '),
  };
  const aliases={mlb:{CWS:'CHW',CHW:'CHW',CHC:'CHC',KC:'KCR',KCR:'KCR',OAK:'ATH',ATH:'ATH',SD:'SDP',SDP:'SDP',SF:'SFG',SFG:'SFG',TB:'TBR',TBR:'TBR',WSH:'WSN',WSN:'WSN'},nfl:{JAC:'JAX',LA:'LAR',LVR:'LV',GNB:'GB',KAN:'KC',NWE:'NE',NOR:'NO',SFO:'SF',TAM:'TB',WAS:'WAS',WSH:'WAS'},nba:{GS:'GSW',NO:'NOP',NY:'NYK',SA:'SAS',WSH:'WAS',UTAH:'UTA'},nhl:{LA:'LAK',NJ:'NJD',SJ:'SJS',TB:'TBL',WAS:'WSH',UTAH:'UTA'}};
  const mlbNames=['Arizona Diamondbacks','Atlanta Braves','Baltimore Orioles','Boston Red Sox','Chicago Cubs','Chicago White Sox','Cincinnati Reds','Cleveland Guardians','Colorado Rockies','Detroit Tigers','Houston Astros','Kansas City Royals','Los Angeles Angels','Los Angeles Dodgers','Miami Marlins','Milwaukee Brewers','Minnesota Twins','New York Mets','New York Yankees','Athletics','Philadelphia Phillies','Pittsburgh Pirates','San Diego Padres','Seattle Mariners','San Francisco Giants','St. Louis Cardinals','Tampa Bay Rays','Texas Rangers','Toronto Blue Jays','Washington Nationals'];
  const nameAliases=new Map();mlbNames.forEach((name,index)=>{const code=teamCodes.mlb[index];for(const variant of[name,name.split(' ').slice(name.includes('Red Sox')||name.includes('White Sox')? -2:-1).join(' '),code])nameAliases.set('mlb:'+normalize(variant),code);});
  const canonicalCode=(league,value)=>aliases[league]?.[String(value).toUpperCase()]||String(value).toUpperCase();
  // Team name facts from ESPN's public /sports/{league}/teams endpoints, captured 2026-09-27.
  const officialNames={"nfl":{"ARI":["Arizona Cardinals","Cardinals","Cardinals"],"ATL":["Atlanta Falcons","Falcons","Falcons"],"BAL":["Baltimore Ravens","Ravens","Ravens"],"BUF":["Buffalo Bills","Bills","Bills"],"CAR":["Carolina Panthers","Panthers","Panthers"],"CHI":["Chicago Bears","Bears","Bears"],"CIN":["Cincinnati Bengals","Bengals","Bengals"],"CLE":["Cleveland Browns","Browns","Browns"],"DAL":["Dallas Cowboys","Cowboys","Cowboys"],"DEN":["Denver Broncos","Broncos","Broncos"],"DET":["Detroit Lions","Lions","Lions"],"GB":["Green Bay Packers","Packers","Packers"],"HOU":["Houston Texans","Texans","Texans"],"IND":["Indianapolis Colts","Colts","Colts"],"JAX":["Jacksonville Jaguars","Jaguars","Jaguars"],"KC":["Kansas City Chiefs","Chiefs","Chiefs"],"LV":["Las Vegas Raiders","Raiders","Raiders"],"LAC":["Los Angeles Chargers","Chargers","Chargers"],"LAR":["Los Angeles Rams","Rams","Rams"],"MIA":["Miami Dolphins","Dolphins","Dolphins"],"MIN":["Minnesota Vikings","Vikings","Vikings"],"NE":["New England Patriots","Patriots","Patriots"],"NO":["New Orleans Saints","Saints","Saints"],"NYG":["New York Giants","Giants","Giants"],"NYJ":["New York Jets","Jets","Jets"],"PHI":["Philadelphia Eagles","Eagles","Eagles"],"PIT":["Pittsburgh Steelers","Steelers","Steelers"],"SF":["San Francisco 49ers","49ers","49ers"],"SEA":["Seattle Seahawks","Seahawks","Seahawks"],"TB":["Tampa Bay Buccaneers","Buccaneers","Buccaneers"],"TEN":["Tennessee Titans","Titans","Titans"],"WSH":["Washington Commanders","Commanders","Commanders"]},"nba":{"ATL":["Atlanta Hawks","Hawks","Hawks"],"BOS":["Boston Celtics","Celtics","Celtics"],"BKN":["Brooklyn Nets","Nets","Nets"],"CHA":["Charlotte Hornets","Hornets","Hornets"],"CHI":["Chicago Bulls","Bulls","Bulls"],"CLE":["Cleveland Cavaliers","Cavaliers","Cavaliers"],"DAL":["Dallas Mavericks","Mavericks","Mavericks"],"DEN":["Denver Nuggets","Nuggets","Nuggets"],"DET":["Detroit Pistons","Pistons","Pistons"],"GS":["Golden State Warriors","Warriors","Warriors"],"HOU":["Houston Rockets","Rockets","Rockets"],"IND":["Indiana Pacers","Pacers","Pacers"],"LAC":["LA Clippers","Clippers","Clippers"],"LAL":["Los Angeles Lakers","Lakers","Lakers"],"MEM":["Memphis Grizzlies","Grizzlies","Grizzlies"],"MIA":["Miami Heat","Heat","Heat"],"MIL":["Milwaukee Bucks","Bucks","Bucks"],"MIN":["Minnesota Timberwolves","Timberwolves","Timberwolves"],"NO":["New Orleans Pelicans","Pelicans","Pelicans"],"NY":["New York Knicks","Knicks","Knicks"],"OKC":["Oklahoma City Thunder","Thunder","Thunder"],"ORL":["Orlando Magic","Magic","Magic"],"PHI":["Philadelphia 76ers","76ers","76ers"],"PHX":["Phoenix Suns","Suns","Suns"],"POR":["Portland Trail Blazers","Trail Blazers","Trail Blazers"],"SAC":["Sacramento Kings","Kings","Kings"],"SA":["San Antonio Spurs","Spurs","Spurs"],"TOR":["Toronto Raptors","Raptors","Raptors"],"UTAH":["Utah Jazz","Jazz","Jazz"],"WSH":["Washington Wizards","Wizards","Wizards"]},"nhl":{"ANA":["Anaheim Ducks","Ducks","Ducks"],"BOS":["Boston Bruins","Bruins","Bruins"],"BUF":["Buffalo Sabres","Sabres","Sabres"],"CGY":["Calgary Flames","Flames","Flames"],"CAR":["Carolina Hurricanes","Hurricanes","Hurricanes"],"CHI":["Chicago Blackhawks","Blackhawks","Blackhawks"],"COL":["Colorado Avalanche","Avalanche","Avalanche"],"CBJ":["Columbus Blue Jackets","Blue Jackets","Blue Jackets"],"DAL":["Dallas Stars","Stars","Stars"],"DET":["Detroit Red Wings","Red Wings","Red Wings"],"EDM":["Edmonton Oilers","Oilers","Oilers"],"FLA":["Florida Panthers","Panthers","Panthers"],"LA":["Los Angeles Kings","Kings","Kings"],"MIN":["Minnesota Wild","Wild","Wild"],"MTL":["Montreal Canadiens","Canadiens","Canadiens"],"NSH":["Nashville Predators","Predators","Predators"],"NJ":["New Jersey Devils","Devils","Devils"],"NYI":["New York Islanders","Islanders","Islanders"],"NYR":["New York Rangers","Rangers","Rangers"],"OTT":["Ottawa Senators","Senators","Senators"],"PHI":["Philadelphia Flyers","Flyers","Flyers"],"PIT":["Pittsburgh Penguins","Penguins","Penguins"],"SJ":["San Jose Sharks","Sharks","Sharks"],"SEA":["Seattle Kraken","Kraken","Kraken"],"STL":["St. Louis Blues","Blues","Blues"],"TB":["Tampa Bay Lightning","Lightning","Lightning"],"TOR":["Toronto Maple Leafs","Maple Leafs","Maple Leafs"],"UTAH":["Utah Mammoth","Mammoth","Mammoth"],"VAN":["Vancouver Canucks","Canucks","Canucks"],"VGK":["Vegas Golden Knights","Golden Knights","Golden Knights"],"WSH":["Washington Capitals","Capitals","Capitals"],"WPG":["Winnipeg Jets","Jets","Jets"]}};
  for(const [league,teams]of Object.entries(officialNames))for(const [abbreviation,names]of Object.entries(teams))for(const name of names.filter(Boolean))nameAliases.set(league+':'+normalize(name),canonicalCode(league,abbreviation));
  function identity(item){
    const symbol=item.symbol||'',event=item.event_id||symbol;
    let league,teams;
    if(item.provider==='polymarket_us'){
      const match=symbol.match(/^(?:aec-)?(mlb|nfl|nba|nhl)-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d{2}-\d{2}(?:-|$)/i);
      if(match){league=match[1].toLowerCase();teams=match.slice(2,4).map(value=>canonicalCode(league,value));}
    }else{
      const match=event.match(/^KX(MLB|NFL|NBA|NHL)[A-Z0-9]*-\d{2}[A-Z]{3}\d{2}(?:\d{4})?([A-Z]+)(?:-|$)/);
      if(match){league=match[1].toLowerCase();const codes=[...teamCodes[league],...Object.keys(aliases[league]||{})],pairs=new Map();
        for(const a of codes)for(const b of codes)if(a!==b&&a+b===match[2]){const pair=[canonicalCode(league,a),canonicalCode(league,b)];pairs.set(pair.join(':'),pair);}
        if(pairs.size===1)teams=[...pairs.values()][0];
      }
    }
    if(teams?.some(code=>!teamCodes[league]?.includes(code)))teams=null;
    if(!teams){league='names';teams=(item.home_team&&item.away_team?[item.home_team,item.away_team]:String(item.event_title||'').split(':')[0].split(/\s+(?:vs\.?|v\.?|@)\s+/i)).map(normalize);}
    const key=teams.length===2&&teams.every(Boolean)?[league,...teams.slice().sort(),item.game_start].join('|'):[item.provider,item.event_id||item.event_title,item.game_start].join('|');
    return {key,league,teams};
  }
  function outcomeTeam(value,info){
    const normalized=normalize(value),code=canonicalCode(info.league,value);
    if(info.teams.includes(code))return code;
    if(info.teams.includes(nameAliases.get(info.league+':'+normalized)))return nameAliases.get(info.league+':'+normalized);
    return info.teams.find(team=>normalize(team)===normalized)||null;
  }
  function group(items){
    const games=new Map();
    for(const item of items){
      const info=identity(item);let game=games.get(info.key);
      if(!game){game={id:info.key,event_title:item.event_title,game_start:item.game_start,generated_at:item.generated_at,info,providers:[],markets:[],native:new Map(),search:[]};games.set(info.key,game);}
      if(item.provider==='polymarket_us')game.event_title=item.event_title;
      if(item.generated_at>game.generated_at)game.generated_at=item.generated_at;
      if(!game.providers.includes(item.provider))game.providers.push(item.provider);
      game.search.push(item.event_title,item.market_title,item.outcome,item.symbol,item.contract_id,item.event_slug,item.provider==='kalshi'?'Kalshi':'Polymarket');
      const nativeKey=item.provider+':'+(item.symbol||item.market_id||item.id);
      let market=game.native.get(nativeKey);if(!market){market={provider:item.provider,symbol:item.symbol,title:item.market_title,rows:[]};game.native.set(nativeKey,market);}market.rows.push(item);
    }
    for(const game of games.values()){
      const propositions=new Map();
      const add=(key,title,provider,yes,no)=>{let market=propositions.get(key);if(!market){market={id:key,title,variants:{}};propositions.set(key,market);}market.variants[provider]={yes,no};};
      for(const [nativeKey,native]of game.native){
        const yes=native.rows.find(row=>['yes','long'].includes(row.side)),no=native.rows.find(row=>['no','short'].includes(row.side));
        const kalshiWinner=native.provider==='kalshi'&&/^KX(?:MLB|NFL|NBA|NHL)GAME-/.test(native.symbol||'');
        const polyWinner=native.provider==='polymarket_us'&&(/full_game_winner/i.test(native.rows[0].market_type||'')||/^(?:aec-)?(?:mlb|nfl|nba|nhl)-[^-]+-[^-]+-\d{4}-\d{2}-\d{2}$/.test(native.symbol||''));
        const affirmative=outcomeTeam(yes?.outcome,game.info)||(kalshiWinner?outcomeTeam(native.symbol.split('-').at(-1),game.info):null);
        const teamLabel=team=>game.info.league==='mlb'?mlbNames[teamCodes.mlb.indexOf(team)]||team:yes?.outcome||team;
        if(affirmative&&(kalshiWinner||polyWinner)){
          add('win:'+affirmative,`${teamLabel(affirmative)} to win`,native.provider,yes,no);
          // A published opposite team contract is the No side only for a two-team winner market.
          const opposite=polyWinner&&outcomeTeam(no?.outcome,game.info);
          if(opposite&&opposite!==affirmative)add('win:'+opposite,`${game.info.league==='mlb'?mlbNames[teamCodes.mlb.indexOf(opposite)]:no.outcome} to win`,native.provider,no,yes);
        }else{
          add(nativeKey,native.title||yes?.outcome||native.rows[0].outcome||game.event_title,native.provider,yes||(!no?native.rows[0]:null),no);
        }
      }
      game.markets=[...propositions.values()].sort((a,b)=>Object.keys(b.variants).length-Object.keys(a.variants).length||a.title.localeCompare(b.title));
      game.search.push(...game.markets.map(m=>m.title));
      game.providers.sort();game.provider=game.providers.join(',');game.search=normalize(game.search.join(' '));delete game.native;
    }
    return [...games.values()];
  }
  const selections=new Map();let cardSequence=0;
  function filter(items,options={}){
    const quantile=options.quantile||'0.5',comparison=options.comparison||'any';
    let query=String(options.search||'').trim(),link;
    if(/^https?:\/\//i.test(query)){
      try{const url=new URL(query);if(url.protocol!=='https:'||!['kalshi.com','www.kalshi.com','polymarket.us','www.polymarket.us'].includes(url.hostname))return [];
        link={provider:url.hostname.includes('kalshi')?'kalshi':'polymarket_us',id:url.hostname.includes('kalshi')&&url.searchParams.get('op_market_ticker')||url.pathname.split('/').filter(Boolean).at(-1)};query='';
      }catch{return [];}
    }
    const rows=items.filter(row=>{
      if(options.provider&&options.provider!=='all'&&row.provider!==options.provider)return false;
      if(link){const strip=s=>String(s||'').toLowerCase().replace(/^aec-/,'');if(row.provider!==link.provider||![row.symbol,row.event_id,row.event_slug].some(s=>strip(s)===strip(link.id)))return false;}
      if(comparison==='any')return true;
      const p=row.latest_price,q=row.endpoint?.[quantile];return Number.isFinite(p)&&Number.isFinite(q)&&(comparison==='below'?p<q:p>q);
    });
    const tokens=normalize(query).split(' ').filter(Boolean),matches=value=>tokens.every(token=>normalize(value).includes(token));
    return group(rows).filter(game=>matches(game.search)).map(game=>{
      const specific=game.markets.filter(m=>tokens.length&&matches(m.title));if(specific.length)game.markets=specific;
      game.providers=[...new Set(game.markets.flatMap(m=>Object.keys(m.variants)))].sort();game.provider=game.providers.join(',');
      game.filtered=comparison!=='any';return game;
    });
  }
  function choices(legend,name,options,onChange){
    const field=el('fieldset','','game-switch');field.append(el('legend',legend));
    for(const option of options){const wrap=el('label','','game-switch-option'),input=el('input'),span=el('span',option.label);input.type='radio';input.name=name;input.value=option.value;input.checked=option.checked;input.disabled=option.disabled||false;input.addEventListener('change',()=>{if(input.checked)onChange(input.value);});wrap.append(input,span);field.append(wrap);}return field;
  }
  function render(entries,options={}){
    // Terminal mounts its screener asynchronously, after this shared script has loaded.
    const cards=document.getElementById('qs-games');if(!cards)return;
    const games=entries[0]?.markets?entries:group(entries);cards.replaceChildren();
    for(const game of games){
      const card=el('article','','game-card'),number=++cardSequence;
      const preferred=game.markets[0],stored=selections.get(game.id);
      let selected=stored||{provider:preferred.variants.kalshi?'kalshi':game.providers[0],market:preferred.id,side:'yes'};
      if(!game.providers.includes(selected.provider))selected.provider=preferred.variants.kalshi?'kalshi':game.providers[0];
      const draw=focusName=>{
        let market=game.markets.find(m=>m.id===selected.market&&m.variants[selected.provider])||game.markets.find(m=>m.variants[selected.provider]);
        selected.market=market.id;const pair=market.variants[selected.provider];
        if(!pair[selected.side])selected.side=pair.yes?'yes':'no';selections.set(game.id,{...selected});
        const row=pair[selected.side];card.replaceChildren();
        const title=el('h3',game.event_title);title.id='game-title-'+number;card.setAttribute('aria-labelledby',title.id);card.append(title,el('p',`Starts ${time(game.game_start)}`,'small'));
        card.append(choices('Platform','game-provider-'+number,[{value:'kalshi',label:'Kalshi',checked:selected.provider==='kalshi',disabled:!game.providers.includes('kalshi')},{value:'polymarket_us',label:'Polymarket',checked:selected.provider==='polymarket_us',disabled:!game.providers.includes('polymarket_us')}],value=>{selected.provider=value;draw('game-provider-'+number);}));
        const marketLabel=el('label','Market','game-market-label'),select=el('select');select.id='game-market-'+number;select.name='game-market-'+number;marketLabel.htmlFor=select.id;
        for(const candidate of game.markets.filter(m=>m.variants[selected.provider])){const option=el('option',candidate.title);option.value=candidate.id;option.selected=candidate.id===market.id;select.append(option);}
        select.addEventListener('change',()=>{selected.market=select.value;draw(select.name);});card.append(marketLabel,select);
        card.append(choices('Position','game-side-'+number,[{value:'yes',label:'Yes',checked:selected.side==='yes',disabled:!pair.yes},{value:'no',label:'No',checked:selected.side==='no',disabled:!pair.no}],value=>{selected.side=value;draw('game-side-'+number);}));
        card.append(provider(row),el('p',`${selected.side==='yes'?'Yes':'No'} · ${market.title}`,'game-selected-outcome'));
        const q=options.quantile||'0.5',price=row.latest_price,threshold=row.endpoint?.[q];
        if(Number.isFinite(price)){
          const cents=v=>(100*v).toFixed(1)+'¢',relation=price>threshold?'above':price<threshold?'below':'equal to';
          card.append(el('p',`Latest price: ${cents(price)}`+(Number.isFinite(threshold)?` · ${label(q)}: ${cents(threshold)} · ${relation==='equal to'?relation:(100*Math.abs(price-threshold)).toFixed(1)+'¢ '+relation}`:` · ${label(q)} unavailable`),'game-price-comparison'));
          if(row.price_checked_at)card.append(el('p',`${row.price_kind==='ask'?'Ask':row.price_kind==='last_trade'?'Last trade':'Market quote'} · checked ${time(row.price_checked_at)}`,'small'));
        }else card.append(el('p',options.checkingPrices?'Checking latest price…':'Latest price unavailable.','small'));
        const summary=el('p',`End-of-horizon P50: ${percent(row.endpoint?.['0.5'])}`,'game-probability');summary.setAttribute('aria-live','polite');card.append(summary);
        const quantiles=el('dl','','game-quantile-summary');for(const q of bands){const value=el('div');value.append(el('dt',label(q)),el('dd',percent(row.endpoint?.[q])));quantiles.append(value);}card.append(quantiles);
        card.append(el('p',`${row.models?.length||0} models · ${row.status==='final_pregame'?'Final pregame forecast':'Updates hourly before start'} · ${row.recomputed_at?'Recomputed':'Updated'} ${time(row.recomputed_at||row.generated_at)}`,'small'));
        if(!pair.yes||!pair.no)card.append(el('p',game.filtered?`${pair.yes?'No':'Yes'} forecast does not match these price filters.`:`${pair.yes?'No':'Yes'} forecast is not available for this market.`,'small'));
        const links=el('div','','game-market-links');
        for(const platform of game.providers){
          const variants=market.variants[platform]||game.markets.find(m=>m.variants[platform])?.variants[platform],contract=variants?.[selected.side]||variants?.yes||variants?.no;
          if(!contract?.market_url)continue;
          try{const url=new URL(contract.market_url),host=platform==='kalshi'?'kalshi.com':'polymarket.us';if(url.protocol!=='https:'||url.hostname!==host)continue;
            const a=el('a',`Open ${platform==='kalshi'?'Kalshi':'Polymarket'} market`,'game-exchange-link');a.href=url.href;a.target='_blank';a.rel='noopener noreferrer';a.setAttribute('aria-label',`${a.textContent} for ${market.title} (opens in a new tab)`);links.append(a);
          }catch{}
        }if(links.children.length)card.append(links);
        const link=el('a','View forecast','cta secondary');link.href=href(row.id);link.setAttribute('aria-label',`View ${selected.side} forecast for ${market.title} on ${selected.provider==='kalshi'?'Kalshi':'Polymarket'}`);card.append(link);
        if(focusName)card.querySelector(`input[name="${focusName}"]:checked,select[name="${focusName}"]`)?.focus();
      };draw();cards.append(card);
    }
  }
  function gameChart(item,history,showHistory,hours){
    const rows=item.predictions,cutoff=Date.parse(item.input_cutoff),last=Date.parse(rows.at(-1).timestamp);
    const observations=showHistory?history.filter(r=>!hours||Date.parse(r.timestamp)>=cutoff-hours*3600000):[];
    const first=observations.length?Date.parse(observations[0].timestamp):cutoff;
    const chart=document.createElementNS('http://www.w3.org/2000/svg','svg');chart.setAttribute('viewBox','0 0 720 310');chart.setAttribute('role','img');chart.setAttribute('aria-label','Genuine observed hourly prices and forecast: P01–P99 outer band, P25–P75 inner band and P50 median. The vertical line marks the original data cutoff. Missing historical hours are not connected. Values follow in the tables.');
    const fontSize=12*720/Math.max(280,Math.min(720,(page?.clientWidth||752)-32));
    const left=Math.max(52,fontSize*3.8+10);
    const x=t=>left+(692-left)*(Date.parse(t)-first)/Math.max(1,last-first),y=p=>242-208*p;
    const svg=(tag,attrs,text)=>{const n=document.createElementNS(chart.namespaceURI,tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);if(text)n.textContent=text;chart.append(n);return n;};
    for(const p of[0,.25,.5,.75,1]){svg('line',{x1:left,x2:692,y1:y(p),y2:y(p),stroke:'var(--border)'});svg('text',{x:left-10,y:y(p)+4,'text-anchor':'end',fill:'var(--muted-foreground)','font-size':fontSize},`${p*100}%`);}
    for(const[lo,hi,opacity]of[['0.01','0.99',.12],['0.25','0.75',.23]])if(rows.every(r=>Number.isFinite(r.quantiles[lo])&&Number.isFinite(r.quantiles[hi])))svg('polygon',{points:[...rows.map(r=>`${x(r.timestamp)},${y(r.quantiles[hi])}`),...rows.slice().reverse().map(r=>`${x(r.timestamp)},${y(r.quantiles[lo])}`)].join(' '),fill:'var(--primary)',opacity});
    svg('polyline',{points:rows.map(r=>`${x(r.timestamp)},${y(r.quantiles['0.5'])}`).join(' '),fill:'none',stroke:'var(--accent-foreground)','stroke-width':3,'stroke-dasharray':'7 3'});
    if(observations.length){
      let path='';observations.forEach((r,i)=>{const connected=i&&Date.parse(r.timestamp)-Date.parse(observations[i-1].timestamp)<=3600000;path+=`${connected?'L':'M'}${x(r.timestamp)},${y(r.price)} `;const dot=svg('circle',{cx:x(r.timestamp),cy:y(r.price),r:2.2,fill:'var(--foreground)','data-observed-history':''});const title=document.createElementNS(chart.namespaceURI,'title');title.textContent=`${time(r.timestamp)} · ${percent(r.price)}`;dot.append(title);});
      svg('path',{d:path,fill:'none',stroke:'var(--foreground)','stroke-width':2,'data-observed-history':''});
    }
    svg('line',{x1:x(item.input_cutoff),x2:x(item.input_cutoff),y1:26,y2:242,stroke:'var(--muted-foreground)','stroke-dasharray':'4 4','data-forecast-cutoff':''});
    svg('text',{x:x(item.input_cutoff),y:18,'text-anchor':x(item.input_cutoff)>560?'end':'start',fill:'var(--muted-foreground)','font-size':fontSize},'Forecast cutoff');
    const axisDate=new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',timeZone:'America/New_York'}),axisClock=new Intl.DateTimeFormat(undefined,{hour:'numeric',minute:'2-digit',timeZone:'America/New_York'});
    for(const [position,timestamp,anchor]of [[left,first,'start'],[692,last,'end']]){for(const [height,formatter]of [[264,axisDate],[294,axisClock]])svg('text',{x:position,y:height,'text-anchor':anchor,fill:'var(--muted-foreground)','font-size':fontSize},formatter.format(new Date(timestamp)));}
    return chart;
  }
  async function show(id,saved=false){
    if(!page){window.location.href=href(id);return;}
    const run=++sequence;viewed=null;page.hidden=false;page.setAttribute('aria-busy','true');page.replaceChildren(el('p','Loading saved game forecast…'));
    try{
      const headers={};if(saved){const user=window.firebase?.auth?.().currentUser;if(!user||user.isAnonymous)throw Error();headers.Authorization=`Bearer ${await (window.QuanturaAuth?.getToken(user) ?? user.getIdToken())}`;}
      const response=await fetch(`/api/screener/games/${saved?'saved/':''}${encodeURIComponent(id)}`,{headers,signal:AbortSignal.timeout(20000)});if(!response.ok)throw Error();const {item}=await response.json();if(run!==sequence)return;
      const rows=item.predictions;if(!Array.isArray(rows)||!rows.length)throw Error();const available=bands.filter(q=>rows.every(r=>Number.isFinite(r.quantiles?.[q])));if(!available.includes('0.5'))throw Error();
      page.replaceChildren(provider(item),el('h2',item.event_title),el('p',item.outcome),el('p',`Game starts ${time(item.game_start)}. Forecast ends ${time(item.forecast_end)}.`),el('p',`${item.recomputed_at?'Recomputed':'Updated'} ${time(item.recomputed_at||item.generated_at)} · ${item.history_count} genuine hourly observations · ${item.models.join(' + ')}`,'small'),el('p',`Pregame data through ${time(item.input_cutoff)} · ${item.recomputed_at?'Retrospective recalculation from the original pregame cutoff.':item.status==='final_pregame'?'Final pregame forecast.':'Updates hourly until the start hour.'}`,'small muted'));
      if(available.length!==6)page.append(el('p','This older snapshot has fewer quantiles. The next successful ensemble refresh will replace it.','notice small'));
      let history=Array.isArray(item.observations)?item.observations:[];
      const controls=el('div','','game-history-controls'),toggleLabel=el('label'),toggle=el('input');toggle.type='checkbox';toggle.checked=true;toggle.name='showGameHistory';toggleLabel.append(toggle,el('span','Show historical prices'));
      const rangeLabel=el('label','History window'),range=el('select');range.name='gameHistoryWindow';for(const [value,title]of[['12','Last 12 hours'],['24','Last 24 hours'],['0','All available history']]){const option=el('option',title);option.value=value;range.append(option);}rangeLabel.append(range);controls.append(toggleLabel,rangeLabel);
      const chartHost=el('div','','game-chart'),historyStatus=el('p',history.length?`${history.length} saved hourly observations.`:'Loading observed hourly prices…','small');historyStatus.setAttribute('role','status');
      const historyDetails=el('details'),historySummary=el('summary','Observed hourly prices'),historyScroll=el('div','','game-table-scroll');historyDetails.append(historySummary,historyScroll);
      const drawChart=()=>{
        const legend=el('div','','game-chart-legend');legend.append(el('span',toggle.checked&&history.length?'Solid line: observed hourly prices':'Historical prices hidden or unavailable'),el('span','Dashed line: forecast P50'));
        chartHost.replaceChildren(gameChart(item,history,toggle.checked,Number(range.value)),legend);
        historyDetails.hidden=!history.length;historySummary.textContent=`Observed hourly prices · ${history.length} observations`;
        const table=el('table'),head=el('thead'),tr=el('tr');for(const title of['Time','Observed price']){const th=el('th',title);th.scope='col';tr.append(th);}head.append(tr);table.append(head);
        const body=el('tbody');for(const row of history){const tr=el('tr');tr.append(el('td',time(row.timestamp)),el('td',percent(row.price)));body.append(tr);}table.append(body);historyScroll.replaceChildren(table);
      };toggle.addEventListener('change',drawChart);range.addEventListener('change',drawChart);drawChart();page.append(controls,chartHost,historyStatus,historyDetails);
      const loadHistory=async()=>{
        try{const response=await fetch(`/api/screener/games/${saved?'saved/':''}${encodeURIComponent(id)}/history`,{headers,signal:AbortSignal.timeout(20000)});if(!response.ok)throw Error();const value=await response.json();if(run!==sequence)return;
          history=(value.observations||[]).filter(row=>Number.isFinite(row.price)&&row.price>=0&&row.price<=1&&Date.parse(row.timestamp)<=Math.min(Date.now(),Date.parse(item.forecast_end)));
          historyStatus.textContent=history.length?`${history.length} genuine hourly observations${value.history_source.startsWith('saved_model_input')?' · saved model inputs': ' · provider history'}${history.some(row=>Date.parse(row.timestamp)>Date.parse(item.input_cutoff))?' + observed outcomes':''} · checked ${time(value.observed_at||new Date().toISOString())}.`:'No genuine hourly history is available for this snapshot.';drawChart();
        }catch{if(run!==sequence)return;historyStatus.replaceChildren(el('span','Historical prices could not be loaded. '));const retry=el('button','Retry history','cta secondary small');retry.type='button';retry.addEventListener('click',loadHistory);historyStatus.append(retry);}
      };void loadHistory();
      const actions=el('div','','game-forecast-actions'),back=el('a','Back to Screener','cta secondary');back.href='/forecasting?panel=screener';actions.append(back);
      const fresh=el('button','New forecast','cta secondary');fresh.type='button';fresh.addEventListener('click',()=>{leaveSavedView();window.__quanturaSetPanel?.('forecast');document.getElementById('market-search-query')?.focus();});actions.append(fresh);
      const refresh=el('button',saved?'Reload saved forecast':'Refresh forecast','cta secondary');refresh.type='button';refresh.addEventListener('click',()=>show(id,saved));actions.append(refresh);
      const download=el('button','Download CSV','cta secondary');download.type='button';download.addEventListener('click',()=>{const csv=['timestamp,'+available.map(label).join(','),...rows.map(r=>[r.timestamp,...available.map(q=>r.quantiles[q])].join(','))].join('\r\n');const url=URL.createObjectURL(new Blob([csv],{type:'text/csv'})),a=el('a');a.href=url;a.download=`${item.provider}-${id}-pregame.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});actions.append(download);page.append(actions);
      const table=el('table','','game-probabilities');table.append(el('caption','Selected outcome probability · forecast quantiles'));const head=el('thead'),tr=el('tr');for(const title of['Time',...available.map(label)]){const th=el('th',title);th.scope='col';tr.append(th);}head.append(tr);table.append(head);
      const body=el('tbody');for(const row of rows){const tr=el('tr');tr.append(el('td',time(row.timestamp)));for(const q of available)tr.append(el('td',percent(row.quantiles[q])));body.append(tr);}table.append(body);const scroll=el('div','','game-table-scroll');scroll.append(table);page.append(scroll);
      const details=el('details');details.append(el('summary','Data and methodology'),el('p',item.method));if(item.schedule_verified_at)details.append(el('p',`Schedule verified ${time(item.schedule_verified_at)} · ${item.schedule_source}`,'small'));for(const warning of item.warnings||[])details.append(el('p',warning,'small'));page.append(details);
      viewed={item,saved};if(!saved)saveViewed();
    }catch{if(run===sequence){page.replaceChildren(el('p',saved?'Sign in to open your saved forecast.':'This forecast could not be loaded.'));const retry=el('button','Try again','cta secondary');retry.type='button';retry.addEventListener('click',()=>show(id,saved));page.append(retry);}}finally{if(run===sequence)page.removeAttribute('aria-busy');}
  }
  window.QuanturaGames=Object.freeze({group,filter,render,show});
  const params=new URLSearchParams(window.location.search),id=params.get('gameForecastId'),savedId=params.get('userGameForecastId');
  if(page&&(/^[a-f0-9]{32}$/.test(id||'')||/^[a-f0-9]{40}$/.test(savedId||''))){
    document.body.classList.add('game-forecast-view');
    if(id)show(id);
    try{window.firebase?.auth?.().onAuthStateChanged(user=>{if(savedId&&new URLSearchParams(window.location.search).get('userGameForecastId')===savedId&&user&&!user.isAnonymous)show(savedId,true);else saveViewed();});}catch{}
    if(savedId&&!window.firebase?.auth?.().currentUser){page.hidden=false;page.replaceChildren(el('p','Sign in to open your saved forecast.'));}
  }
})();
