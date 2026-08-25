/**
 * Dashboard page — T050
 * 4 stat cards, records-by-source table, IPFS/MCP status section.
 */

import React from 'react';
import {
  useReactTable,
  getCoreRowModel,
  createColumnHelper,
  flexRender,
} from '@tanstack/react-table';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Skeleton } from '@/components/ui/skeleton';
import { ExternalLink, Database, Activity, Globe, Server } from 'lucide-react';
import { useStats, useSources, type SourceListItem } from '@/services/api';

// ---------------------------------------------------------------------------
// Stat cards
// ---------------------------------------------------------------------------

function StatCards() {
  const { data: stats, isLoading } = useStats();

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-2">
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-20" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const lastRun = stats?.lastRun;
  const lastRunAgo = lastRun ? formatTimeAgo(new Date(lastRun.started_at)) : 'Never';

  const cards = [
    {
      title: 'Total Properties',
      value: (stats?.totalProperties ?? 0).toLocaleString(),
      sub: lastRun ? `+${lastRun.delta_new} new` : 'No runs yet',
      icon: Database,
    },
    {
      title: 'Last Run',
      value: lastRunAgo,
      sub: lastRun
        ? `+${lastRun.delta_new} / ~${lastRun.delta_updated}`
        : 'No runs yet',
      icon: Activity,
    },
    {
      title: 'IPNS Status',
      value: stats?.ipnsStatus === 'live' ? 'Live' : stats?.ipnsStatus === 'stale' ? 'Stale' : 'Pending',
      sub: stats?.artifactCid ? `CID: ${stats.artifactCid.slice(0, 12)}...` : 'No artifact',
      icon: Globe,
      indicatorColor:
        stats?.ipnsStatus === 'live'
          ? 'bg-green-500'
          : stats?.ipnsStatus === 'stale'
            ? 'bg-yellow-500'
            : 'bg-gray-400',
    },
    {
      title: 'Sources',
      value: `${stats?.healthySources ?? 0}/${stats?.sourceCount ?? 0}`,
      sub: 'healthy',
      icon: Server,
    },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {cards.map((card) => (
        <Card key={card.title}>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">{card.title}</CardTitle>
            <card.icon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              {card.indicatorColor && (
                <span className={`inline-block h-2.5 w-2.5 rounded-full ${card.indicatorColor}`} />
              )}
              <div className="text-2xl font-bold">{card.value}</div>
            </div>
            <p className="text-xs text-muted-foreground">{card.sub}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Records by Source table (TanStack Table)
// ---------------------------------------------------------------------------

const sourceColumnHelper = createColumnHelper<SourceListItem>();

const sourceColumns = [
  sourceColumnHelper.accessor('name', {
    header: 'Source',
    cell: (info) => <span className="font-medium">{info.getValue()}</span>,
  }),
  sourceColumnHelper.accessor('record_count', {
    header: 'Records',
    cell: (info) => info.getValue().toLocaleString(),
  }),
  sourceColumnHelper.accessor('last_successful_run', {
    header: 'Last Collected',
    cell: (info) => {
      const val = info.getValue();
      if (!val) return <span className="text-muted-foreground">Never</span>;
      return new Date(val).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });
    },
  }),
  sourceColumnHelper.accessor('status', {
    header: 'Status',
    cell: (info) => {
      const status = info.getValue();
      const variant =
        status === 'healthy'
          ? 'success'
          : status === 'slow'
            ? 'warning'
            : 'destructive';
      return <Badge variant={variant}>{status}</Badge>;
    },
  }),
];

function SourcesTable() {
  const { data, isLoading } = useSources();
  const sources = data?.sources ?? [];

  const table = useReactTable({
    data: sources,
    columns: sourceColumns,
    getCoreRowModel: getCoreRowModel(),
  });

  if (isLoading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        {table.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id}>
            {headerGroup.headers.map((header) => (
              <TableHead key={header.id}>
                {flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.length === 0 ? (
          <TableRow>
            <TableCell colSpan={sourceColumns.length} className="text-center text-muted-foreground">
              No data sources configured
            </TableCell>
          </TableRow>
        ) : (
          table.getRowModel().rows.map((row) => (
            <TableRow key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <TableCell key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
            </TableRow>
          ))
        )}
      </TableBody>
    </Table>
  );
}

// ---------------------------------------------------------------------------
// IPFS & MCP section
// ---------------------------------------------------------------------------

function IpfsMcpSection() {
  const { data: stats } = useStats();

  const openDataIpns = stats?.ipnsPointer ?? 'Not published';
  const ipnsStatus = stats?.ipnsStatus ?? 'pending';
  const statusColor =
    ipnsStatus === 'live' ? 'text-green-600' : ipnsStatus === 'stale' ? 'text-yellow-600' : 'text-gray-400';
  const statusDot =
    ipnsStatus === 'live' ? 'bg-green-500' : ipnsStatus === 'stale' ? 'bg-yellow-500' : 'bg-gray-400';

  const gatewayUrl = stats?.ipnsPointer
    ? `https://ipfs.filebase.io/ipns/${stats.ipnsPointer}/index.json`
    : null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Elephant IPFS & MCP</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <div className="flex items-center justify-between">
          <span className="text-muted-foreground">Open Data IPNS:</span>
          <div className="flex items-center gap-2">
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              {stats?.ipnsPointer ? truncate(stats.ipnsPointer, 16) : 'N/A'}
            </code>
            <span className={`inline-block h-2 w-2 rounded-full ${statusDot}`} />
            <span className={statusColor}>
              {ipnsStatus === 'live' ? 'Live' : ipnsStatus === 'stale' ? 'Stale' : 'Pending'}
            </span>
            {gatewayUrl && (
              <a
                href={gatewayUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-primary hover:underline"
              >
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            )}
          </div>
        </div>

        <div className="flex items-center justify-between">
          <span className="text-muted-foreground">Query Table IPNS:</span>
          <div className="flex items-center gap-2">
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              {stats?.ipnsPointer ? truncate(stats.ipnsPointer, 16) : 'N/A'}
            </code>
            <span className={`inline-block h-2 w-2 rounded-full ${statusDot}`} />
            <span className={statusColor}>
              {ipnsStatus === 'live' ? 'Live' : ipnsStatus === 'stale' ? 'Stale' : 'Pending'}
            </span>
          </div>
        </div>

        <div className="flex items-center justify-between">
          <span className="text-muted-foreground">MCP Endpoint:</span>
          <div className="flex items-center gap-2">
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">/mcp</code>
            <Badge variant="success">Configured</Badge>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Dashboard page (exported)
// ---------------------------------------------------------------------------

export default function DashboardPage() {
  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold tracking-tight">Dashboard</h2>

      {/* Stat cards */}
      <StatCards />

      {/* Records by Source */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Records by Source</CardTitle>
        </CardHeader>
        <CardContent>
          <SourcesTable />
        </CardContent>
      </Card>

      {/* IPFS & MCP */}
      <IpfsMcpSection />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatTimeAgo(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function truncate(s: string, maxLen: number): string {
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen) + '...';
}
