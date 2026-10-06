import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  kicker: string;
  description: ReactNode;
  to: string;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'CPT as the drum',
    kicker: 'Priority',
    to: '/docs/business-context/ubiquitous-language',
    description: (
      <>
        The <strong>Critical Pull Time</strong> — the last moment a parcel can be
        manifested and still make its truck — is the only sort key. Work is
        handed out earliest-CPT-first, always, because that is the one deadline
        the building cannot negotiate with.
      </>
    ),
  },
  {
    title: 'Waveless release',
    kicker: 'Admission',
    to: '/docs/business-context/why-waveless-release',
    description: (
      <>
        No waves, no batch windows, no scheduler. Work is admitted{' '}
        <strong>one unit at a time, on demand</strong>, and priority is
        re-evaluated at every single call — so a re-timed truck changes the very
        next handout.
      </>
    ),
  },
  {
    title: 'WIP-limit backpressure',
    kicker: 'Invariant',
    to: '/docs/ddd/aggregate-design-canvas',
    description: (
      <>
        On a release-fed pool the WIP limit is an{' '}
        <strong>enforceable invariant</strong>, not a guideline: release refuses
        past the ceiling. On a flow-fed pool it is only an alarm — you cannot
        refuse a tote a conveyor already delivered.
      </>
    ),
  },
  {
    title: 'Flow balancing',
    kicker: 'Correction',
    to: '/docs/business-context/flow-balancing',
    description: (
      <>
        Drum-Buffer-Rope over live buffer telemetry. Two pool types get two
        levers: <strong>throttle upstream</strong> when you do not control
        arrivals, <strong>flag a headcount move</strong> when you already do.
      </>
    ),
  },
];

function Feature({title, kicker, description, to}: FeatureItem) {
  return (
    <div className={clsx('col col--6', styles.featureCol)}>
      <Link to={to} className={styles.featureCard}>
        <span className={styles.featureKicker}>{kicker}</span>
        <Heading as="h3" className={styles.featureTitle}>
          {title}
        </Heading>
        <p className={styles.featureBody}>{description}</p>
      </Link>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
