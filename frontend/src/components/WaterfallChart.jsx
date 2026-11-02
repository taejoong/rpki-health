import { useMemo } from 'react';
import { CHART_COLORS } from './ui';

export default function WaterfallChart({ entries }) {
  const { chartData, timeTicks, uniqueAsns } = useMemo(() => {
    if (!entries || entries.length === 0) return { chartData: [], timeTicks: [], uniqueAsns: [] };

    const times = entries.flatMap((e) => [
      e.first_seen ? new Date(e.first_seen).getTime() : null,
      e.last_seen ? new Date(e.last_seen).getTime() : null,
    ]).filter(Boolean);
    const minTime = Math.min(...times);
    const maxTime = Math.max(...times);
    const range = maxTime - minTime || 1;

    const data = entries.map((e, i) => {
      const start = e.first_seen ? new Date(e.first_seen).getTime() : minTime;
      const end = e.last_seen ? new Date(e.last_seen).getTime() : maxTime;
      return {
        ...e, _idx: i,
        _startPct: ((start - minTime) / range) * 100,
        _widthPct: Math.max(((end - start) / range) * 100, 0.5),
        _startDate: e.first_seen?.slice(0, 10) || '?',
        _endDate: e.last_seen?.slice(0, 10) || '?',
        _days: Math.round((end - start) / 86400000),
      };
    });

    const ticks = [];
    for (let i = 0; i <= 6; i++) {
      const t = minTime + (range * i) / 6;
      const d = new Date(t);
      ticks.push({ pct: (i / 6) * 100, label: `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}` });
    }

    return { chartData: data, timeTicks: ticks, uniqueAsns: [...new Set(data.map((d) => d.asn))] };
  }, [entries]);

  if (chartData.length === 0) return null;

  const TimeAxis = () => (
    <div className="relative h-5 ml-28 mr-16">
      {timeTicks.map((tick, i) => (
        <div key={i} className="absolute flex flex-col items-center" style={{ left: `${tick.pct}%`, transform: 'translateX(-50%)' }}>
          <div className="w-px h-2 bg-cf-border" />
          <span className="text-[10px] text-cf-text-muted mt-0.5">{tick.label}</span>
        </div>
      ))}
    </div>
  );

  return (
    <div className="space-y-1 py-2">
      <TimeAxis />
      <div className="mb-1" />
      {uniqueAsns.map((asn, asnIdx) => {
        const rows = chartData.filter((d) => d.asn === asn);
        return (
          <div key={asn} className="flex items-center gap-2 h-6 group">
            <div className="w-24 text-right text-xs font-mono text-cf-text-secondary shrink-0 truncate">AS{asn}</div>
            <div className="flex-1 relative bg-cf-surface-2 rounded h-4">
              {rows.map((d, i) => (
                <div key={i} className="absolute top-0 h-4 rounded" style={{
                  left: `${d._startPct}%`, width: `${d._widthPct}%`,
                  backgroundColor: CHART_COLORS[asnIdx % CHART_COLORS.length], minWidth: '4px', opacity: 0.85,
                }} title={`AS${d.asn} | ${d._startDate} → ${d._endDate} (${d._days}d)`} />
              ))}
            </div>
            <div className="w-14 text-[10px] text-cf-text-muted shrink-0 opacity-0 group-hover:opacity-100">
              {rows.length > 1 ? `${rows.length} ROAs` : `${rows[0]?._days}d`}
            </div>
          </div>
        );
      })}
      <div className="mt-1 border-t border-cf-border" />
      <TimeAxis />
    </div>
  );
}
