import React from 'react';
import {AbsoluteFill, interpolate, useCurrentFrame} from 'remotion';

export const EndCard: React.FC = () => {
  const frame = useCurrentFrame();
  return <AbsoluteFill style={{fontFamily: 'Arial, sans-serif', background: 'linear-gradient(135deg, #f4f8ff 0%, #e7efff 58%, #d8e7ff 100%)', color: '#10223d', padding: 72}}>
    <div style={{position: 'absolute', right: -130, top: -200, width: 850, height: 850, borderRadius: '50%', background: 'radial-gradient(circle, #adcaff80, #adcaff00 68%)'}}/>
    <svg viewBox="0 0 650 520" style={{position: 'absolute', width: 660, height: 530, right: -15, top: 120}} aria-hidden="true">
      <defs><linearGradient id="end-band" x1="0" y1="0" x2="0" y2="1"><stop stopColor="#5c8ff1" stopOpacity=".35"/><stop offset="1" stopColor="#5c8ff1" stopOpacity=".04"/></linearGradient></defs>
      {[160,240,320,400].map(y=><path key={y} d={'M80 '+y+'H590'} stroke="#87a9da" strokeOpacity=".22"/>)}
      <path d="M80 335C160 330 200 310 260 290S400 240 580 100L580 405C400 340 340 340 260 345S160 350 80 335Z" fill="url(#end-band)"/>
      <path d="M80 335C160 330 200 310 260 290S400 240 580 100" fill="none" stroke="#7da5e8" strokeWidth="2"/>
      <path d="M80 335C160 330 200 310 260 290S400 260 580 225" fill="none" stroke="#2563c6" strokeWidth="6" strokeLinecap="round"/>
      <circle cx="580" cy="225" r="10" fill="#2563c6"/><circle cx="580" cy="225" r="22" fill="none" stroke="#2563c6" strokeOpacity=".2" strokeWidth="8"/>
    </svg>
    <div style={{position: 'relative', opacity: interpolate(frame, [0, 12], [0, 1], {extrapolateRight: 'clamp'})}}>
      <div style={{display: 'flex', alignItems: 'center', gap: 14, fontSize: 25, fontWeight: 700, letterSpacing: 5}}><span style={{display: 'inline-block', width: 34, height: 34, border: '7px solid #2563c6', borderRadius: '50%'}}/>QUANTURA</div>
      <div style={{fontSize: 74, fontWeight: 700, letterSpacing: -3, lineHeight: 1.06, marginTop: 100, maxWidth: 680}}>See the range.<br/><span style={{color: '#2563c6'}}>Find your next idea.</span></div>
      <p style={{fontSize: 27, lineHeight: 1.4, color: '#465b78', maxWidth: 560, margin: '28px 0 32px'}}>Forecast stocks and prediction markets.<br/>Explore what could come next.</p>
      <div style={{display: 'inline-flex', alignItems: 'center', gap: 22, background: '#2563c6', color: '#ffffff', padding: '18px 26px', borderRadius: 14, fontSize: 27, fontWeight: 700, boxShadow: '0 10px 28px #2563c626'}}>Start exploring <span>↗</span></div>
    </div>
    <div style={{position: 'absolute', left: 72, bottom: 42, fontSize: 22, color: '#465b78'}}>quantura.studio</div>
    <div style={{position: 'absolute', right: 72, bottom: 42, fontSize: 16, color: '#465b78'}}>Illustrative forecast · Outcomes are uncertain</div>
  </AbsoluteFill>;
};
