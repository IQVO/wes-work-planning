import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns (WMS/WES/WCS, waveless
      release, CloudEvents). Not a production system. Not affiliated with,
      endorsed by, or representative of Amazon or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.heroKicker}>
          warehouse-systems · WES tier · Core subdomain
        </p>
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className={clsx('hero__subtitle', styles.heroSubtitle)}>
          {siteConfig.tagline}
        </p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/overview/">
            Read the documentation
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/api/rest/wes-work-planning-release">
            API Reference
          </Link>
        </div>
      </div>
    </header>
  );
}

function TheLoop() {
  return (
    <section className={styles.loop}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          The loop, end to end
        </Heading>
        <p className={styles.sectionLead}>
          One closed control loop: a shift&rsquo;s charge becomes a plan, the plan
          becomes released work, and completion feedback reopens capacity for the
          next release.
        </p>
        <ol className={styles.loopList}>
          <li>
            <strong>Charge</strong> — volume due by each CPT arrives for a
            process path.
          </li>
          <li>
            <strong>Plan</strong> — rate &times; heads &times; hours is committed,
            never exceeding installed stations.
          </li>
          <li>
            <strong>Release</strong> — the earliest-CPT unit is admitted, at most
            once, bounded by the pool&rsquo;s WIP limit.
          </li>
          <li>
            <strong>Observe</strong> — backlog depth and WIP are projected live
            from pool state.
          </li>
          <li>
            <strong>Correct</strong> — throttle upstream, or flag a headcount
            move. Then round again.
          </li>
        </ol>
      </div>
    </section>
  );
}

function Wiring() {
  return (
    <section className={styles.wiring}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Four of its integration edges
        </Heading>
        <div className="row">
          <div className="col col--3">
            <div className={styles.wireCard}>
              <span className={styles.wireDir}>consumes</span>
              <code>ShiftPlanCommitted</code>
              <p>from workforce-management</p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.wireCard}>
              <span className={styles.wireDir}>consumes</span>
              <code>StockReserved</code>
              <p>from inventory-storage</p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.wireCard}>
              <span className={styles.wireDir}>publishes</span>
              <code>WorkReleased</code>
              <p>to fulfillment-execution</p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.wireCard}>
              <span className={styles.wireDir}>consumes</span>
              <code>TaskCompleted</code>
              <p>from fulfillment-execution</p>
            </div>
          </div>
        </div>
        <p className={styles.wiringFoot}>
          It also consumes order-management's <code>OrderAllocated</code>,
          process-path-management's catalogue, network-inventory-planning's{' '}
          <code>WorkDemandReleased</code> and, into a local copy,
          product-master's <code>ProductClassified</code>. Every consumer path
          is idempotent under at-least-once redelivery.{' '}
          <Link to="/docs/ecosystem/context-map">See the full context map →</Link>
        </p>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Documentation"
      description={`Documentation for ${siteConfig.title} — the core bounded context of the WES tier: charge, plan, waveless release, and flow balancing.`}>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <TheLoop />
        <Wiring />
      </main>
    </Layout>
  );
}
