import React from 'react';
import {AbsoluteFill, useCurrentFrame, interpolate} from 'remotion';
export const Frame: React.FC<React.PropsWithChildren> = ({children}) => {
 const frame=useCurrentFrame();
 return <AbsoluteFill style={{background:'#07101f',color:'#edf3fb',fontFamily:'Arial, sans-serif',padding:72,opacity:interpolate(frame,[0,12,168,180],[0,1,1,0],{extrapolateLeft:'clamp',extrapolateRight:'clamp'})}}>
  <div style={{fontSize:22,fontWeight:700,letterSpacing:5,color:'#a9b7ca'}}>QUANTURA</div>{children}<div style={{position:'absolute',bottom:48,right:72,fontSize:18,color:'#a9b7ca'}}>quantura.studio</div>
 </AbsoluteFill>;
};
