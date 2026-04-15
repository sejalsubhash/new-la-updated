import React, { useState, useEffect, useCallback } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { jobsApi } from '../services/api';
import {
  Plus, FileText, Clock, CheckCircle2, AlertCircle,
  Loader2, TrendingUp, FileWarning, ChevronRight,
  PlayCircle, AlertTriangle, RefreshCw
} from 'lucide-react';

export default function DashboardPage() {
  const [jobs,     setJobs]     = useState([]);
  const [loading,  setLoading]  = useState(true);
  const [error,    setError]    = useState('');
  const [resuming, setResuming] = useState({});
  const [refreshing, setRefreshing] = useState(false);
  const navigate = useNavigate();

  const loadJobs = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    else setRefreshing(true);
    setError('');
    try {
      const response = await jobsApi.list();
      // Handle both { jobs: [...] } and bare array shapes
      const list = Array.isArray(response) ? response : (response.jobs || []);
      setJobs(list);
    } catch (err) {
      console.error('[Dashboard] loadJobs error:', err);
      setError(`Failed to load jobs: ${err.message}`);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // Initial load
  useEffect(() => { loadJobs(); }, [loadJobs]);

  // Auto-refresh every 30 s if any job is actively running
  useEffect(() => {
    const hasActive = jobs.some(j =>
      ['extracting', 'processing', 'analyzing', 'generating-report'].includes(j.status)
    );
    if (!hasActive) return;
    const t = setInterval(() => loadJobs(true), 30000);
    return () => clearInterval(t);
  }, [jobs, loadJobs]);

  /* ── Resume a job directly from the dashboard ── */
  const handleResume = async (e, jobId) => {
    e.preventDefault();
    e.stopPropagation();
    setResuming(prev => ({ ...prev, [jobId]: true }));
    try {
      await jobsApi.resumeJob(jobId);
      navigate(`/job/${jobId}`);
    } catch (err) {
      alert(`Failed to resume: ${err.message}`);
      setResuming(prev => ({ ...prev, [jobId]: false }));
    }
  };

  /* ── Status badge ── */
  const getStatusBadge = (status) => {
    const badges = {
      created:             { color: 'bg-gray-100 text-gray-700',    icon: Clock,          label: 'Created' },
      uploaded:            { color: 'bg-blue-100 text-blue-700',    icon: FileText,       label: 'Uploaded' },
      extracting:          { color: 'bg-amber-100 text-amber-700',  icon: Loader2,        label: 'Extracting' },
      extracted:           { color: 'bg-amber-100 text-amber-700',  icon: FileText,       label: 'Extracted' },
      processing:          { color: 'bg-purple-100 text-purple-700',icon: Loader2,        label: 'Processing' },
      analyzing:           { color: 'bg-purple-100 text-purple-700',icon: Loader2,        label: 'Analyzing' },
      'analysis-complete': { color: 'bg-green-100 text-green-700',  icon: CheckCircle2,   label: 'Analyzed' },
      'generating-report': { color: 'bg-blue-100 text-blue-700',    icon: Loader2,        label: 'Generating' },
      completed:           { color: 'bg-green-100 text-green-700',  icon: CheckCircle2,   label: 'Completed' },
      failed:              { color: 'bg-red-100 text-red-700',      icon: AlertCircle,    label: 'Failed' },
      interrupted:         { color: 'bg-amber-100 text-amber-700',  icon: AlertTriangle,  label: 'Interrupted' },
    };
    const badge = badges[status] || badges.created;
    const Icon  = badge.icon;
    const spin  = ['extracting', 'processing', 'analyzing', 'generating-report'].includes(status);
    return (
      <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${badge.color}`}>
        <Icon className={`w-3.5 h-3.5 ${spin ? 'animate-spin' : ''}`} />
        {badge.label}
      </span>
    );
  };

  /* ── Can this job be resumed? ── */
  const canResume = (job) => {
    const nonResumable = new Set(['extracting', 'processing', 'analyzing', 'generating-report', 'completed', 'failed', 'created']);
    return !nonResumable.has(job.status) && (job.totalDocuments || 0) > 0;
  };

  const effectiveStatus = (job) => canResume(job) ? 'interrupted' : job.status;

  /* ── Summary stats ── */
  const stats = {
    total:             jobs.length,
    completed:         jobs.filter(j => j.status === 'completed').length,
    processing:        jobs.filter(j => ['processing', 'analyzing', 'extracting'].includes(j.status)).length,
    documentsAnalyzed: jobs
      .filter(j => j.status === 'completed')
      .reduce((sum, j) => sum + (j.processedCount || j.totalDocuments || 0), 0),
  };

  /* ── Loading skeleton ── */
  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">

      {/* ── Header ── */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-display font-bold text-gray-900">Dashboard</h1>
          <p className="text-gray-500 mt-1">Overview of your legal audit jobs</p>
        </div>
        <Link to="/new-job" className="btn-primary inline-flex items-center gap-2">
          <Plus className="w-5 h-5" /> New Audit Job
        </Link>
      </div>

      {/* ── Stats cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Jobs',       value: stats.total,             bg: 'bg-primary-100', icon: FileText,    iconColor: 'text-primary-600' },
          { label: 'Completed',        value: stats.completed,         bg: 'bg-green-100',   icon: CheckCircle2,iconColor: 'text-green-600' },
          { label: 'In Progress',      value: stats.processing,        bg: 'bg-amber-100',   icon: Loader2,     iconColor: 'text-amber-600' },
          { label: 'Docs Analyzed',    value: stats.documentsAnalyzed, bg: 'bg-purple-100',  icon: TrendingUp,  iconColor: 'text-purple-600' },
        ].map(({ label, value, bg, icon: Icon, iconColor }) => (
          <div key={label} className="card p-5">
            <div className="flex items-center gap-3">
              <div className={`w-10 h-10 ${bg} rounded-lg flex items-center justify-center`}>
                <Icon className={`w-5 h-5 ${iconColor}`} />
              </div>
              <div>
                <p className="text-2xl font-bold text-gray-900">{value}</p>
                <p className="text-sm text-gray-500">{label}</p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* ── Jobs table ── */}
      <div className="card">
        <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between">
          <h2 className="font-semibold text-gray-900">
            Recent Jobs
            {jobs.length > 0 && (
              <span className="ml-2 text-sm font-normal text-gray-400">({jobs.length})</span>
            )}
          </h2>
          <button
            onClick={() => loadJobs(true)}
            disabled={refreshing}
            className="inline-flex items-center gap-1.5 text-sm text-primary-600 hover:text-primary-700 disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>

        {/* Error banner */}
        {error && (
          <div className="mx-4 mt-4 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm flex items-center gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            {error}
            <button onClick={() => loadJobs()} className="ml-auto underline">Retry</button>
          </div>
        )}

        {/* Empty state */}
        {jobs.length === 0 && !error ? (
          <div className="p-12 text-center">
            <FileWarning className="w-12 h-12 text-gray-300 mx-auto mb-4" />
            <h3 className="font-medium text-gray-900 mb-1">No jobs yet</h3>
            <p className="text-gray-500 text-sm mb-4">Create your first legal audit job to get started</p>
            <Link to="/new-job" className="btn-primary inline-flex items-center gap-2">
              <Plus className="w-4 h-4" /> Create Job
            </Link>
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {jobs.map((job) => {
              const status     = effectiveStatus(job);
              const resumable  = canResume(job);
              const isResuming = resuming[job.id];

              return (
                <div
                  key={job.id}
                  className="flex items-center justify-between px-6 py-4 hover:bg-gray-50 transition-colors"
                >
                  {/* Left — job info */}
                  <Link to={`/job/${job.id}`} className="flex items-center gap-4 flex-1 min-w-0">
                    <div className={`w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0 ${
                      status === 'completed'   ? 'bg-green-100' :
                      status === 'interrupted' ? 'bg-amber-100' :
                      status === 'failed'      ? 'bg-red-100'   : 'bg-gray-100'
                    }`}>
                      <FileText className={`w-5 h-5 ${
                        status === 'completed'   ? 'text-green-600' :
                        status === 'interrupted' ? 'text-amber-600' :
                        status === 'failed'      ? 'text-red-600'   : 'text-gray-500'
                      }`} />
                    </div>
                    <div className="min-w-0">
                      <p className="font-medium text-gray-900 truncate">{job.id}</p>
                      <div className="flex items-center gap-2 text-sm text-gray-500 flex-wrap">
                        <span>
                          {new Date(job.createdAt).toLocaleDateString('en-IN', {
                            day: 'numeric', month: 'short', year: 'numeric',
                            hour: '2-digit', minute: '2-digit',
                          })}
                        </span>
                        {job.createdBy && job.createdBy !== 'unknown' && (
                          <><span>·</span><span>{job.createdBy}</span></>
                        )}
                        {job.completedAt && (
                          <>
                            <span>·</span>
                            <span className="text-green-600">
                              Done {new Date(job.completedAt).toLocaleDateString('en-IN', {
                                day: 'numeric', month: 'short',
                                hour: '2-digit', minute: '2-digit',
                              })}
                            </span>
                          </>
                        )}
                      </div>
                    </div>
                  </Link>

                  {/* Right — stats + badge + action */}
                  <div className="flex items-center gap-4 flex-shrink-0 ml-4">
                    <div className="text-right hidden sm:block">
                      <p className="text-sm font-medium text-gray-900">
                        {job.processedCount || 0}
                        {job.totalDocuments ? ` / ${job.totalDocuments}` : ''} docs
                      </p>
                      <p className="text-xs text-gray-500">
                        {job.failedCount ? `${job.failedCount} flagged` : 'No failures'}
                      </p>
                    </div>

                    {getStatusBadge(status)}

                    {resumable ? (
                      <button
                        onClick={(e) => handleResume(e, job.id)}
                        disabled={isResuming}
                        className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-white text-xs font-semibold rounded-lg transition-all flex-shrink-0 disabled:opacity-60"
                        title="Resume processing from checkpoint"
                      >
                        {isResuming
                          ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          : <PlayCircle className="w-3.5 h-3.5" />}
                        Resume
                      </button>
                    ) : (
                      <Link to={`/job/${job.id}`}>
                        <ChevronRight className="w-5 h-5 text-gray-400" />
                      </Link>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
