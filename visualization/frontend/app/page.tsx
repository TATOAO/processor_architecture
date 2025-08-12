// @ts-nocheck
"use client";

import { useEffect, useMemo, useState, useCallback } from 'react';
import GraphFullDemo, { type GraphData } from './components/GraphFullDemo';

export default function Home() {
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const backendBaseUrl = useMemo(
    () => process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000',
    []
  );

  useEffect(() => {
    let aborted = false;
    async function bootstrap() {
      try {
        const sessionResp = await fetch(`${backendBaseUrl}/api/demo/session`);
        const sessionJson = (await sessionResp.json()) as { session_id: string };
        if (aborted) return;
        setSessionId(sessionJson.session_id);

        const graphResp = await fetch(`${backendBaseUrl}/api/demo/graph`);
        const graphJson = (await graphResp.json()) as GraphData;
        if (aborted) return;
        setGraphData(graphJson);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('Failed to load initial data', err);
      }
    }
    bootstrap();
    return () => {
      aborted = true;
    };
  }, [backendBaseUrl]);

  const handleGraphChange = useCallback(
    async (newGraphData: GraphData) => {
      setGraphData(newGraphData);
      if (!sessionId) return;
      try {
        await fetch(`${backendBaseUrl}/api/sessions/${sessionId}/graph`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(newGraphData),
        });
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('Failed to persist graph', err);
      }
    },
    [backendBaseUrl, sessionId]
  );

  return (
    <main className="min-h-screen p-6">
      <h1 className="text-2xl font-bold mb-4">Graph Full Demo</h1>
      {graphData ? (
        <GraphFullDemo graphData={graphData} onGraphChange={handleGraphChange} />
      ) : (
        <div>Loading...</div>
      )}
    </main>
  );
}
