import assert from "node:assert/strict";
import test from "node:test";
import {companyDomain,companyLogo} from "./companyLogos";
test("company logo lookup accepts only exact ticker matches and a public domain",async()=>{
  assert.equal(companyDomain("https://www.apple.com/investors"),"apple.com");
  for(const value of ["http://localhost/logo","http://127.0.0.1/logo","https://user:pass@apple.com","javascript:alert(1)"])assert.equal(companyDomain(value),null);
  let calls=0;const request=(async()=>{calls++;return Response.json({results:[{symbol:"FUZZY",website:"wrong.com"},{symbol:"EXACT",website:"https://www.apple.com"}]});}) as typeof fetch;
  const first=await companyLogo("EXACT",request);assert.equal(first,"apple.com");assert.equal(await companyLogo("EXACT",request),first);assert.equal(calls,1);
  assert.equal(await companyLogo("OTHER",request),null);
});
