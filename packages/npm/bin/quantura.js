#!/usr/bin/env node
import {parseArgs} from 'node:util';
import {promises as fs} from 'node:fs';
import {Quantura} from '../src/index.js';
import {login,logout,accessToken} from '../src/oauth.js';

const {values,positionals}=parseArgs({allowPositionals:true,options:{help:{type:'boolean',short:'h'},'no-browser':{type:'boolean'},
  'base-url':{type:'string'},file:{type:'string'},output:{type:'string'},source:{type:'string'},limit:{type:'string'},key:{type:'string'},wait:{type:'boolean'},'all-pages':{type:'boolean'}}});
const [command,...args]=positionals;
async function main() {
  if(values.help || !command) {console.log(`Quantura 1.0.0\n\nquantura login [--no-browser]\nquantura logout\nquantura whoami\nquantura search AAPL [--source alpaca]\nquantura models\nquantura forecast --file request.json [--key retry-key]\nquantura get FORECAST_ID [--wait]\nquantura download FORECAST_ID --output forecast.csv\nquantura history --file history.json [--all-pages] [--output data.json]\nquantura scout --file question.json\n\nUse OAuth login or QUANTURA_API_KEY. API access requires paid Pro or administrator access.`);return;}
  if(command==='login'){await login({noBrowser:values['no-browser']});console.log('Signed in to Quantura.');return;}
  if(command==='logout'){const revoked=await logout();console.log(revoked?'Signed out; OAuth grant revoked.':'Local credentials removed. Remote revocation was not confirmed.');return;}
  const client=new Quantura({baseUrl:values['base-url'],token:()=>accessToken(values['base-url'])});
  const body=async()=>{if(!values.file)throw new Error('Provide --file with a JSON request.');return JSON.parse(await fs.readFile(values.file,'utf8'));};
  let result;
  switch(command){
    case 'whoami':result=await client.access();break;
    case 'search':if(!args.length)throw new Error('Provide a search query.');result=await client.search(args.join(' '),{source:values.source,limit:values.limit?Number(values.limit):undefined});break;
    case 'models':result=await client.models();break;
    case 'forecast':result=await client.createForecast(await body(),{idempotencyKey:values.key});break;
    case 'get':if(!args[0])throw new Error('Provide a forecast ID.');result=values.wait?await client.waitForForecast(args[0]):await client.getForecast(args[0]);break;
    case 'download':if(!args[0] || !values.output)throw new Error('Provide a forecast ID and --output.');result=await client.downloadForecast(args[0]);break;
    case 'history':{const request=await body();if(values['all-pages']){result=[];for await(const page of client.historyPages(request))result.push(page);}else result=await client.history(request);break;}
    case 'scout':{const q=await body();result=await client.askScout(q.context,q.question,{conversationId:q.conversation_id,turnId:q.turn_id});break;}
    default:throw new Error('Unknown command. Run quantura --help.');
  }
  const output=typeof result==='string'?result:JSON.stringify(result,null,2)+'\n';
  if(values.output){await fs.writeFile(values.output,output);console.error('Saved '+values.output);}else process.stdout.write(output);
}
main().catch(error=>{console.error(`${error.code || 'ERROR'}: ${error.message}${error.requestId?' · Reference '+error.requestId:''}`);process.exitCode=1;});
