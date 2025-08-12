'use client';

import React, { useCallback, type ReactElement } from 'react';
import {
  ReactFlow,
  addEdge,
  Background,
  Controls,
  MiniMap,
  useEdgesState,
  useNodesState,
  Connection,
  Edge,
  Node,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

const initialNodes: Node[] = [
  { id: 'A', position: { x: 0, y: 100 }, data: { label: 'Node A' } },
  { id: 'B', position: { x: 250, y: 0 }, data: { label: 'Node B' } },
  { id: 'C', position: { x: 250, y: 200 }, data: { label: 'Node C' } },
];

const initialEdges: Edge[] = [
  { id: 'eA-B', source: 'A', target: 'B' },
  { id: 'eA-C', source: 'A', target: 'C' },
];

export default function GraphDemo(): ReactElement {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(initialEdges);

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  return (
    <div style={{ width: '100%', height: 600 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        fitView
      >
        <Controls />
        <MiniMap />
        <Background />
      </ReactFlow>
    </div>
  );
}


