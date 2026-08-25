/**
 * Property Search page — query selector dropdown, results table, export CSV.
 * T054 — 6 query types, TanStack Table, click row opens property detail drawer.
 */

import React, { useState, useMemo, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
} from '@tanstack/react-table';
import { PropertyDetailDrawer } from '../components/property-detail-drawer.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface QueryType {
  type: string;
  label: string;
  signal_label: string;
}

interface SearchResult {
  parcel_id: string;
  address: string;
  assessed_value: number | null;
  signal_value: string | null;
  source_count: number;
  provenance: Record<string, unknown>;
  derived_signals: Record<string, unknown>;
}

interface SearchResponse {
  query_type: string;
  query_label: string;
  signal_label: string;
  total: number;
  page: number;
  limit: number;
  pages: number;
  results: SearchResult[];
}

const QUERY_TYPES: QueryType[] = [
  { type: 'roof_age_gt_15', label: 'Roofs older than 15 years', signal_label: 'Roof Age (yrs)' },
  { type: 'water_view', label: 'View of water', signal_label: 'Water Distance (ft)' },
  { type: 'ownership_tenure_gt_10', label: 'No ownership change in 10+ years', signal_label: 'Tenure (yrs)' },
  { type: 'regional_owners', label: 'Regional owners', signal_label: 'Owner Location' },
  { type: 'transit_walking', label: 'Walking distance to public transit', signal_label: 'Transit (mi)' },
  { type: 'starbucks_walking', label: 'Walking distance to Starbucks', signal_label: 'Starbucks (mi)' },
];

// ---------------------------------------------------------------------------
// API fetch
// ---------------------------------------------------------------------------

async function fetchSearch(queryType: string, page: number): Promise<SearchResponse> {
  const res = await fetch(`/api/properties/search?query=${queryType}&page=${page}&limit=50`);
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// CSV export
// ---------------------------------------------------------------------------

function exportCsv(data: SearchResponse) {
  const headers = ['Parcel ID', 'Address', 'Value', data.signal_label, 'Sources'];
  const rows = data.results.map((r) => [
    r.parcel_id,
    r.address ?? '',
    r.assessed_value != null ? String(r.assessed_value) : '',
    r.signal_value ?? '',
    String(r.source_count),
  ]);

  const csv = [headers, ...rows].map((row) => row.map((c) => `"${c}"`).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `property-search-${data.query_type}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function PropertySearchPage() {
  const [selectedQuery, setSelectedQuery] = useState<string>(QUERY_TYPES[0].type);
  const [page, setPage] = useState(1);
  const [selectedParcelId, setSelectedParcelId] = useState<string | null>(null);

  const currentQueryDef = QUERY_TYPES.find((q) => q.type === selectedQuery) ?? QUERY_TYPES[0];

  const { data, isLoading, error } = useQuery({
    queryKey: ['property-search', selectedQuery, page],
    queryFn: () => fetchSearch(selectedQuery, page),
  });

  const columns = useMemo<ColumnDef<SearchResult>[]>(
    () => [
      {
        accessorKey: 'parcel_id',
        header: 'Parcel ID',
        cell: (info) => (
          <span className="font-mono text-sm">{info.getValue<string>()}</span>
        ),
      },
      {
        accessorKey: 'address',
        header: 'Address',
        cell: (info) => info.getValue<string>() || 'N/A',
      },
      {
        accessorKey: 'assessed_value',
        header: 'Value',
        cell: (info) => {
          const val = info.getValue<number | null>();
          if (val == null) return 'N/A';
          return `$${(val / 1000).toFixed(0)}k`;
        },
      },
      {
        accessorKey: 'signal_value',
        header: () => currentQueryDef.signal_label,
        cell: (info) => info.getValue<string>() ?? 'N/A',
      },
      {
        accessorKey: 'source_count',
        header: 'Sources',
        cell: (info) => info.getValue<number>(),
      },
    ],
    [currentQueryDef.signal_label],
  );

  const table = useReactTable({
    data: data?.results ?? [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  const handleQueryChange = useCallback((e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedQuery(e.target.value);
    setPage(1);
  }, []);

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">Property Search</h1>

      {/* Query selector */}
      <div className="mb-6">
        <label htmlFor="query-type" className="block text-sm font-medium text-muted-foreground mb-2">
          Query Selector
        </label>
        <select
          id="query-type"
          value={selectedQuery}
          onChange={handleQueryChange}
          className="w-full max-w-md rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
        >
          {QUERY_TYPES.map((qt) => (
            <option key={qt.type} value={qt.type}>
              {qt.label}
            </option>
          ))}
        </select>
      </div>

      {/* Results header */}
      <div className="flex items-center justify-between mb-4">
        <div className="text-sm text-muted-foreground">
          {data ? (
            <span>{data.total.toLocaleString()} results</span>
          ) : isLoading ? (
            <span>Loading...</span>
          ) : null}
        </div>
        {data && data.results.length > 0 && (
          <button
            onClick={() => exportCsv(data)}
            className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors"
          >
            Export CSV
          </button>
        )}
      </div>

      {/* Error state */}
      {error && (
        <div className="rounded-md border border-destructive/50 bg-destructive/10 p-4 text-sm text-destructive mb-4">
          {String(error)}
        </div>
      )}

      {/* Results table */}
      <div className="rounded-md border">
        <table className="w-full text-sm">
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b bg-muted/50">
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="px-4 py-3 text-left font-medium text-muted-foreground"
                  >
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                  Loading results...
                </td>
              </tr>
            ) : table.getRowModel().rows.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                  No results found
                </td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  onClick={() => setSelectedParcelId(row.original.parcel_id)}
                  className="border-b hover:bg-muted/50 cursor-pointer transition-colors"
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-4 py-3">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {data && data.pages > 1 && (
        <div className="flex items-center justify-between mt-4">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="rounded-md border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
          >
            Previous
          </button>
          <span className="text-sm text-muted-foreground">
            Page {data.page} of {data.pages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(data.pages, p + 1))}
            disabled={page >= data.pages}
            className="rounded-md border border-gray-300 px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
          >
            Next
          </button>
        </div>
      )}

      {/* Property detail drawer */}
      {selectedParcelId && (
        <PropertyDetailDrawer
          parcelId={selectedParcelId}
          onClose={() => setSelectedParcelId(null)}
        />
      )}
    </div>
  );
}

export default PropertySearchPage;
