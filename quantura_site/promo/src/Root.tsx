import React from 'react';
import {Composition, interpolate, staticFile} from 'remotion';
import {Audio} from '@remotion/media';
import {TransitionSeries} from '@remotion/transitions';
import {Intro} from './scenes/Intro';
import {Search} from './scenes/Search';
import {Range} from './scenes/Range';
import {Today} from './scenes/Today';
import {EndCard} from './scenes/EndCard';

const Tour: React.FC = () => <>
  <TransitionSeries>
    {[Intro, Search, Range].map((Scene, i) =>
      <TransitionSeries.Sequence key={i} durationInFrames={180}><Scene/></TransitionSeries.Sequence>
    )}
    <TransitionSeries.Sequence durationInFrames={90}><Today/></TransitionSeries.Sequence>
    <TransitionSeries.Sequence durationInFrames={90}><EndCard/></TransitionSeries.Sequence>
  </TransitionSeries>
  <Audio src={staticFile('quantura-synth-wave.mp3')}
    volume={frame => interpolate(frame, [0, 24, 660, 719], [0, 0.75, 0.75, 0], {extrapolateLeft: 'clamp', extrapolateRight: 'clamp'})}/>
</>;

export const RemotionRoot: React.FC = () =>
  <Composition id="QuanturaIntro" component={Tour} durationInFrames={720} fps={30} width={1280} height={720}/>;
