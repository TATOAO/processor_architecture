'use client';

import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import {
  ReactFlow,
  Node,
  Edge,
  addEdge,
  Connection,
  useNodesState,
  useEdgesState,
  Controls,
  Background,
  MiniMap,
  Panel,
  NodeTypes,
  ReactFlowProvider,
  useReactFlow,
  getBezierPath,
  Position,
  Handle,
  NodeToolbar,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

// Animated edge component with flow animation
const AnimatedEdge = (props: any) => {
  const {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
    data,
  } = props;
  const isAnimating = data?.isAnimating;
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  return (
    <>
      {/* Main edge path - hidden when animating */}
      {!isAnimating && (
        <path
          id={id}
          style={{
            ...style,
            strokeDasharray: '5,5', // Make edges dashed by default
          }}
          className="react-flow__edge-path"
          d={edgePath}
          markerEnd={markerEnd}
        />
      )}
      
      {/* Animated flow overlay - shows arrow when animating */}
      {isAnimating && (
        <path
          id={`${id}-flow`}
          style={{
            stroke: '#3b82f6',
            strokeWidth: 3,
            fill: 'none',
            strokeDasharray: '10,5',
            animation: 'flowAnimation 1s linear infinite',
          }}
          className="react-flow__edge-path"
          d={edgePath}
          markerEnd={markerEnd}
        />
      )}
    </>
  );
};

// Custom node component for processor nodes
const ProcessorNode = ({ data, id }: { data: any; id: string }) => {
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number } | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editName, setEditName] = useState(data.processor_class_name);
  const [showTypeSelector, setShowTypeSelector] = useState(false);
  const nodeRef = useRef<HTMLDivElement>(null);

  // Available processor types
  const processorTypes = [
    { name: 'ChunkerProcessor', uniqueName: 'chunker_processor_1' },
    { name: 'HasherProcessor', uniqueName: 'hasher_processor_1' },
  ];

  const handleContextMenu = (event: React.MouseEvent) => {
    event.preventDefault();
    setContextMenu({ x: event.clientX, y: event.clientY });
  };

  const handleCloseContextMenu = () => {
    setContextMenu(null);
  };

  const handleEditName = () => {
    setIsEditing(true);
    setContextMenu(null);
  };

  const handleSaveName = () => {
    data.processor_class_name = editName;
    setIsEditing(false);
  };

  const handleDelete = () => {
    if (data.onDelete) {
      data.onDelete(id);
    }
    setContextMenu(null);
  };

  const handleAddConnectedNode = () => {
    if (data.onAddConnectedNode) {
      data.onAddConnectedNode(id);
    }
    setContextMenu(null);
  };

  const handleChangeProcessorType = (processorType: { name: string; uniqueName: string }) => {
    data.processor_class_name = processorType.name;
    data.processor_unique_name = processorType.uniqueName;
    setShowTypeSelector(false);
  };

  return (
    <>
      <NodeToolbar
        isVisible={data.forceToolbarVisible || undefined}
        position={Position.Right}
        className="bg-white border border-gray-300 rounded-lg shadow-lg p-1"
      >
        <div className="flex gap-1">
          <button
            onClick={handleAddConnectedNode}
            className="px-3 py-1 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm font-medium"
            title="Add connected node"
          >
            Add
          </button>
          <button
            onClick={handleDelete}
            className="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600 text-sm font-medium"
            title="Delete node"
          >
            Delete
          </button>
        </div>
      </NodeToolbar>

      <NodeToolbar
        isVisible={data.forceToolbarVisible || undefined}
        position={Position.Top}
        className="bg-white border border-gray-300 rounded-lg shadow-lg p-1"
      >
        <div className="relative">
          <button
            onClick={() => setShowTypeSelector(!showTypeSelector)}
            className="px-3 py-1 bg-green-500 text-white rounded hover:bg-green-600 text-sm font-medium"
            title="Change processor type"
          >
            Type
          </button>
          {showTypeSelector && (
            <div className="absolute top-full left-0 mt-1 bg-white border border-gray-300 rounded-lg shadow-lg py-1 min-w-[200px] z-10">
              {processorTypes.map((type) => (
                <button
                  key={type.name}
                  onClick={() => handleChangeProcessorType(type)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-100 ${
                    data.processor_class_name === type.name ? 'bg-blue-50 text-blue-600' : ''
                  }`}
                >
                  <div className="font-medium">{type.name}</div>
                  <div className="text-xs text-gray-500">({type.uniqueName})</div>
                </button>
              ))}
            </div>
          )}
        </div>
      </NodeToolbar>
      
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#555' }}
      />
      <div
        ref={nodeRef}
        className="px-4 py-2 shadow-md rounded-md bg-white border-2 border-gray-200 cursor-pointer"
        onContextMenu={handleContextMenu}
      >
        <div className="flex items-center">
          <div className="rounded-full w-8 h-8 flex items-center justify-center bg-blue-100">
            <span className="text-blue-600 font-bold text-xs">
              {data.processor_class_name?.split('Processor')[0] || 'P'}
            </span>
          </div>
          <div className="ml-2">
            {isEditing ? (
              <input
                type="text"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                onBlur={handleSaveName}
                onKeyPress={(e) => e.key === 'Enter' && handleSaveName()}
                className="text-lg font-bold border border-gray-300 rounded px-1"
                autoFocus
              />
            ) : (
              <div className="text-lg font-bold">{data.processor_class_name}</div>
            )}
            <div className="text-gray-500 text-sm">{data.processor_unique_name}</div>
          </div>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#555' }}
      />

      {/* Context Menu */}
      {contextMenu && (
        <div
          className="fixed z-50 bg-white border border-gray-300 rounded-lg shadow-lg py-2 min-w-[150px]"
          style={{ left: contextMenu.x, top: contextMenu.y }}
        >
          <button
            onClick={handleEditName}
            className="w-full text-left px-4 py-2 text-sm hover:bg-gray-100"
          >
            Edit Name
          </button>
          <button
            onClick={handleAddConnectedNode}
            className="w-full text-left px-4 py-2 text-sm hover:bg-gray-100"
          >
            Add Connected Node
          </button>
          <button
            onClick={handleDelete}
            className="w-full text-left px-4 py-2 text-sm hover:bg-gray-100 text-red-600"
          >
            Delete Node
          </button>
        </div>
      )}

      {/* Overlay to close context menu when clicking outside */}
      {contextMenu && (
        <div
          className="fixed inset-0 z-40"
          onClick={handleCloseContextMenu}
        />
      )}

      {/* Overlay to close type selector when clicking outside */}
      {showTypeSelector && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setShowTypeSelector(false)}
        />
      )}
    </>
  );
};

const nodeTypes: NodeTypes = {
  processor: ProcessorNode,
};

// Use AnimatedEdge for all edges
const edgeTypes = {
  default: AnimatedEdge,
};

interface GraphData {
  nodes: Array<{
    processor_class_name: string;
    processor_unique_name: string;
  }>;
  edges: Array<{
    source_node_unique_name: string;
    target_node_unique_name: string;
    edge_unique_name: string;
  }>;
  processor_id: string;
}

interface GraphComponentProps {
  graphData: GraphData;
  onGraphChange?: (newGraphData: GraphData) => void;
}

function GraphComponentInner({ graphData, onGraphChange }: GraphComponentProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [isAnimating, setIsAnimating] = useState(false);

  const deleteNode = useCallback((nodeId: string) => {
    setNodes((nds) => nds.filter((node) => node.id !== nodeId));
    setEdges((eds) => eds.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
    
    // Update parent component
    if (onGraphChange) {
      const newGraphData = {
        ...graphData,
        nodes: graphData.nodes.filter((node) => node.processor_unique_name !== nodeId),
        edges: graphData.edges.filter((edge) => 
          edge.source_node_unique_name !== nodeId && edge.target_node_unique_name !== nodeId
        ),
      };
      onGraphChange(newGraphData);
    }
  }, [setNodes, setEdges, graphData, onGraphChange]);

  const addConnectedNode = useCallback((sourceNodeId: string) => {
    const newNodeId = `processor_${Date.now()}`;
    const newNode: Node = {
      id: newNodeId,
      type: 'processor',
      position: { x: Math.random() * 500, y: Math.random() * 300 },
      data: {
        processor_class_name: 'NewProcessor',
        processor_unique_name: newNodeId,
        onDelete: deleteNode,
        onAddConnectedNode: addConnectedNode,
      },
    };
    setNodes((nds) => [...nds, newNode]);
    
    // Add edge from source to new node
    const newEdge: Edge = {
      id: `edge_${Date.now()}`,
      source: sourceNodeId,
      target: newNodeId,
      type: 'default',
      markerEnd: 'url(#arrowhead)',
      style: { strokeWidth: 2, stroke: '#b1b1b7' },
    };
    setEdges((eds) => [...eds, newEdge]);
    
    // Update parent component
    if (onGraphChange) {
      const newGraphData = {
        ...graphData,
        nodes: [
          ...graphData.nodes,
          {
            processor_class_name: 'NewProcessor',
            processor_unique_name: newNodeId,
          },
        ],
        edges: [
          ...graphData.edges,
          {
            source_node_unique_name: sourceNodeId,
            target_node_unique_name: newNodeId,
            edge_unique_name: newEdge.id,
          },
        ],
      };
      onGraphChange(newGraphData);
    }
  }, [setNodes, setEdges, graphData, onGraphChange, deleteNode]);

  // Convert graph data to xyflow format
  const initialNodes: Node[] = useMemo(() => {
    return graphData.nodes.map((node, index) => ({
      id: node.processor_unique_name,
      type: 'processor',
      // Increased x spacing and staggered y
      position: { x: 400 * (index + 1), y: 100 + (index % 2) * 120 },
      data: {
        ...node,
        onDelete: deleteNode,
        onAddConnectedNode: addConnectedNode,
      },
    }));
  }, [graphData.nodes, deleteNode, addConnectedNode]);

  const initialEdges: Edge[] = useMemo(() => {
    return graphData.edges.map((edge) => ({
      id: edge.edge_unique_name,
      source: edge.source_node_unique_name,
      target: edge.target_node_unique_name,
      type: 'default',
      markerEnd: 'url(#arrowhead)',
      style: { strokeWidth: 2, stroke: '#b1b1b7' },
    }));
  }, [graphData.edges]);

  // Initialize nodes and edges when graphData changes
  React.useEffect(() => {
    console.log('Setting nodes:', initialNodes);
    console.log('Setting edges:', initialEdges);
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  const onConnect = useCallback(
    (params: Connection) => {
      const newEdge: Edge = {
        id: `edge_${Date.now()}`,
        source: params.source!,
        target: params.target!,
        type: 'default',
        markerEnd: 'url(#arrowhead)',
        style: { strokeWidth: 2, stroke: '#b1b1b7' },
      };
      setEdges((eds) => addEdge(newEdge, eds));
      
      // Update parent component
      if (onGraphChange) {
        const newGraphData = {
          ...graphData,
          edges: [
            ...graphData.edges,
            {
              source_node_unique_name: params.source!,
              target_node_unique_name: params.target!,
              edge_unique_name: newEdge.id,
            },
          ],
        };
        onGraphChange(newGraphData);
      }
    },
    [setEdges, graphData, onGraphChange]
  );

  const addNode = useCallback(() => {
    const newNodeId = `processor_${Date.now()}`;
    const newNode: Node = {
      id: newNodeId,
      type: 'processor',
      position: { x: Math.random() * 500, y: Math.random() * 300 },
      data: {
        processor_class_name: 'NewProcessor',
        processor_unique_name: newNodeId,
        onDelete: deleteNode,
        onAddConnectedNode: addConnectedNode,
      },
    };
    setNodes((nds) => [...nds, newNode]);
    
    // Update parent component
    if (onGraphChange) {
      const newGraphData = {
        ...graphData,
        nodes: [
          ...graphData.nodes,
          {
            processor_class_name: 'NewProcessor',
            processor_unique_name: newNodeId,
          },
        ],
      };
      onGraphChange(newGraphData);
    }
  }, [setNodes, graphData, onGraphChange, deleteNode, addConnectedNode]);

  const deleteSelected = useCallback(() => {
    setNodes((nds) => nds.filter((node) => !node.selected));
    setEdges((eds) => eds.filter((edge) => !edge.selected));
  }, [setNodes, setEdges]);

  const startAnimation = useCallback(() => {
    setIsAnimating(true);
  }, []);

  const stopAnimation = useCallback(() => {
    setIsAnimating(false);
  }, []);

  return (
    <div className="w-full h-[600px] border border-gray-300 rounded-lg">
      <div className="p-2 bg-gray-100 text-xs">
        Debug: Nodes: {nodes.length}, Edges: {edges.length}
      </div>
      <ReactFlow
        nodes={nodes}
        edges={edges.map(edge => ({
          ...edge,
          data: { isAnimating }
        }))}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.2, minZoom: 0.5, maxZoom: 1.5 }}
        defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
        attributionPosition="bottom-left"
        defaultEdgeOptions={{
          type: 'default',
          markerEnd: 'url(#arrowhead)',
          style: { strokeWidth: 2, stroke: '#b1b1b7' },
        }}
      >
        <svg width="0" height="0">
          <defs>
            <marker
              id="arrowhead"
              markerWidth="10"
              markerHeight="7"
              refX="10"
              refY="3.5"
              orient="auto"
              markerUnits="strokeWidth"
            >
              <polygon points="0 0, 10 3.5, 0 7" fill="#b1b1b7" />
            </marker>
          </defs>
        </svg>
        
        {/* CSS for flow animation */}
        <style>
          {`
            @keyframes flowAnimation {
              0% {
                stroke-dashoffset: 15;
              }
              100% {
                stroke-dashoffset: 0;
              }
            }
          `}
        </style>
        
        <Controls />
        <Background />
        <MiniMap />
        <Panel position="top-left" className="bg-white p-2 rounded shadow">
          <div className="flex gap-2">
            <button
              onClick={addNode}
              className="px-3 py-1 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm"
            >
              Add Node
            </button>
            <button
              onClick={deleteSelected}
              className="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600 text-sm"
            >
              Delete Selected
            </button>
            <button
              onClick={startAnimation}
              disabled={isAnimating}
              className={`px-3 py-1 text-white rounded text-sm ${
                isAnimating 
                  ? 'bg-gray-400 cursor-not-allowed' 
                  : 'bg-green-500 hover:bg-green-600'
              }`}
            >
              Run Pipeline
            </button>
            <button
              onClick={stopAnimation}
              disabled={!isAnimating}
              className={`px-3 py-1 text-white rounded text-sm ${
                !isAnimating 
                  ? 'bg-gray-400 cursor-not-allowed' 
                  : 'bg-orange-500 hover:bg-orange-600'
              }`}
            >
              Stop
            </button>
          </div>
        </Panel>
      </ReactFlow>
    </div>
  );
}

export default function GraphComponent(props: GraphComponentProps) {
  return (
    <ReactFlowProvider>
      <GraphComponentInner {...props} />
    </ReactFlowProvider>
  );
}
