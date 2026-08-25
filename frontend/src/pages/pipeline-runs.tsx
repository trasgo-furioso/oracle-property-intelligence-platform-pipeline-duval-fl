/**
 * Pipeline Runs page — T051
 * Trigger Run button, runs table with expandable rows showing source details,
 * published artifact, webhook status, and limitations.
 */

import React, { useState, Fragment } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getExpandedRowModel,
  createColumnHelper,
  flexRender,
  type ExpandedState,
} from '@tanstack/react-table';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Skeleton } from '@/components/ui/skeleton';
import { ChevronRight, ChevronDown, Play, AlertTriangle } from 'lucide-react';
import {
  useRuns,
  useRun,
  useTriggerRun,
  type RunListItem,
  type PipelineRunStatus,
} from '@/services/api';

// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: PipelineRunStatus }) {
  const variants: Record<PipelineRunStatus, { variant: 'success' | 'info' | 'warning' | 'destructive' | 'outline'; label: string }> = {
    success: { variant: 'success', label: 'Success' },
    running: { variant: 'info', label: 'Running' },
    partial: { variant: 'warning', label: 'Partial' },
    failed: { variant: 'destructive', label: 'Failed' },
  };
  const { variant, label } = variants[status] ?? { variant: 'outline' as const, label: status };
  return <Badge variant={variant}>{label}</Badge>;
}

// ---------------------------------------------------------------------------
// Expanded row detail
// ---------------------------------------------------------------------------

function ExpandedRunDetail({ runId }: { runId: string }) {
  const { data: run, isLoading } = useRun(runId);

  if (isLoading) {
    return (
      <div className="space-y-2 p-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} className="h-6 w-full" />
        ))}
      </div>
    );
  }

  if (!run) {
    return <div className="p-4 text-sm text-muted-foreground">Could not load run details.</div>;
  }

  return (
    <div className="space-y-4 rounded-md border bg-muted/30 p-4">
      {/* Sources ingested */}
      <div>
        <h4 className="mb-2 text-sm font-semibold">Sources Ingested:</h4>
        <div className="space-y-1">
          {run.sources.length === 0 ? (
            <span className="text-sm text-muted-foreground">No source data available</span>
          ) : (
            run.sources.map((src) => {
              const avgTime = src.duration_ms != null ? `${(src.duration_ms / 1000).toFixed(1)}s avg` : 'N/A';
              const hasIssues = src.limitations && src.limitations.length > 0;
              return (
                <div key={src.source_id} className="flex items-center gap-3 text-sm">
                  <Badge variant={src.status === 'success' ? 'success' : src.status === 'failed' ? 'destructive' : 'warning'} className="w-20 justify-center text-xs">
                    {src.status}
                  </Badge>
                  <span className="w-28 font-medium">{src.source_name}</span>
                  <span className="w-24 text-muted-foreground">
                    {src.records_ingested.toLocaleString()} rec
                  </span>
                  <span className="w-20 text-muted-foreground">{avgTime}</span>
                  {hasIssues ? (
                    <span className="flex items-center gap-1 text-yellow-600">
                      <AlertTriangle className="h-3.5 w-3.5" />
                      {src.limitations}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">-- no issues</span>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* Published artifact */}
      {(run.published_artifact_cid || run.ipns_pointer) && (
        <div>
          <h4 className="mb-1 text-sm font-semibold">Published Artifact:</h4>
          <div className="space-y-0.5 text-sm">
            {run.published_artifact_cid && (
              <div>
                <span className="text-muted-foreground">CID: </span>
                <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                  {run.published_artifact_cid}
                </code>
              </div>
            )}
            {run.ipns_pointer && (
              <div>
                <span className="text-muted-foreground">IPNS: </span>
                <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
                  {run.ipns_pointer}
                </code>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Limitations */}
      {run.source_limitations.length > 0 && (
        <div>
          <h4 className="mb-1 text-sm font-semibold">Limitations:</h4>
          <ul className="space-y-0.5 text-sm">
            {run.source_limitations.map((lim, i) => (
              <li key={i} className="flex items-start gap-1.5 text-yellow-600">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                {lim}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Runs table (TanStack Table)
// ---------------------------------------------------------------------------

const columnHelper = createColumnHelper<RunListItem>();

const columns = [
  columnHelper.display({
    id: 'expand',
    header: '',
    size: 40,
    cell: ({ row }) => (
      <button
        onClick={row.getToggleExpandedHandler()}
        className="rounded p-1 text-muted-foreground hover:bg-accent"
      >
        {row.getIsExpanded() ? (
          <ChevronDown className="h-4 w-4" />
        ) : (
          <ChevronRight className="h-4 w-4" />
        )}
      </button>
    ),
  }),
  columnHelper.display({
    id: 'run_number',
    header: 'Run',
    cell: ({ row }) => {
      // Show run number based on row index (from bottom up) or use a truncated ID
      const idx = row.index;
      return <span className="font-medium">#{String(idx + 1).padStart(3, '0')}</span>;
    },
  }),
  columnHelper.accessor('started_at', {
    header: 'Timestamp',
    cell: (info) =>
      new Date(info.getValue()).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      }),
  }),
  columnHelper.accessor('delta_new', {
    header: 'New',
    cell: (info) => info.getValue().toLocaleString(),
  }),
  columnHelper.accessor('delta_updated', {
    header: 'Updated',
    cell: (info) => info.getValue().toLocaleString(),
  }),
  columnHelper.accessor('delta_removed', {
    header: 'Removed',
    cell: (info) => info.getValue().toLocaleString(),
  }),
  columnHelper.accessor('status', {
    header: 'Status',
    cell: (info) => <StatusBadge status={info.getValue()} />,
  }),
];

function RunsTable() {
  const [page, setPage] = useState(1);
  const limit = 20;
  const { data, isLoading } = useRuns(page, limit);
  const [expanded, setExpanded] = useState<ExpandedState>({});

  const runs = data?.runs ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / limit));

  const table = useReactTable({
    data: runs,
    columns,
    state: { expanded },
    onExpandedChange: setExpanded,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getRowCanExpand: () => true,
  });

  if (isLoading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <TableHead key={header.id} style={{ width: header.getSize() !== 150 ? header.getSize() : undefined }}>
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows.length === 0 ? (
            <TableRow>
              <TableCell colSpan={columns.length} className="text-center text-muted-foreground">
                No pipeline runs yet
              </TableCell>
            </TableRow>
          ) : (
            table.getRowModel().rows.map((row) => (
              <Fragment key={row.id}>
                <TableRow className="cursor-pointer" onClick={() => row.toggleExpanded()}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
                {row.getIsExpanded() && (
                  <TableRow>
                    <TableCell colSpan={columns.length} className="p-0">
                      <ExpandedRunDetail runId={row.original.run_id} />
                    </TableCell>
                  </TableRow>
                )}
              </Fragment>
            ))
          )}
        </TableBody>
      </Table>

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-muted-foreground">
        <span>
          Showing {runs.length} of {total} runs
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            Previous
          </Button>
          <span className="flex items-center px-2">
            {page} / {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages}
            onClick={() => setPage((p) => p + 1)}
          >
            Next
          </Button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Pipeline Runs page (exported)
// ---------------------------------------------------------------------------

export default function PipelineRunsPage() {
  const triggerMutation = useTriggerRun();

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold tracking-tight">Pipeline Runs</h2>
        <Button
          onClick={() => triggerMutation.mutate('duval')}
          disabled={triggerMutation.isPending}
          className="bg-blue-600 text-white hover:bg-blue-700"
        >
          <Play className="h-4 w-4" />
          {triggerMutation.isPending ? 'Triggering...' : 'Trigger Run'}
        </Button>
      </div>

      <Card>
        <CardContent className="pt-6">
          <RunsTable />
        </CardContent>
      </Card>
    </div>
  );
}
