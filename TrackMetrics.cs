using System;
using System.Diagnostics;
using System.Linq;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Prometheus;

namespace Hangfire.Jobs.Prometheus
{
    public class TrackMetrics : JobFilterAttribute, IElectStateFilter, IServerFilter
    {
        private static readonly Gauge SucceededJobTimestamps = Metrics
            .CreateGauge("hangfire_job_last_success_timestamp_seconds", 
                "The most recent succeeded timestamp for a job on this hangfire server", 
                "jobname", "recurringjobid", "deadline");
        
        private static readonly Counter JobDurationsTotal = Metrics
            .CreateCounter("hangfire_job_duration_seconds_total", 
                "The total amount of seconds a job took on this server", 
                "jobname", "recurringjobid", "timelimit");
        
        private static readonly Counter JobDurationsCount = Metrics
                    .CreateCounter("hangfire_job_duration_seconds_count", 
                        "The total of seconds a job took on this server", 
                        "jobname", "recurringjobid", "timelimit");
        
        private readonly string _jobName;
        private readonly int _deadline;
        private readonly int _timelimit;

        /// <summary>
        /// Tracks the metrics for a job, with the focus on using the statistics in AlertManager
        /// </summary>
        /// <param name="jobName">The unique identifier of the job. Defaults to the method signature without namespace.</param>
        /// <param name="deadline">Time in seconds since the last success.</param>
        /// <param name="timelimit">Amount of time in seconds of the expected maximum duration</param>
        public TrackMetrics(string jobName = null, int deadline = 0, int timelimit = 0)
        {
            _jobName = jobName;
            _deadline = deadline;
            _timelimit = timelimit;
        }

        private string GetJobName(BackgroundJob backgroundJob)
        {
            return _jobName ??
                   $"{backgroundJob.Job.ToString()}" +
                   $"({string.Join(", ", backgroundJob.Job.Method.GetParameters().Select(x => x.ParameterType))})";
        }

        private string GetRecurringJobIdIfApplicable(IStorageConnection connection, string jobId)
        {
            return connection.GetJobParameter(jobId, "RecurringJobId") ?? "";
        }
        
        public void OnStateElection(ElectStateContext context)
        {
            var jobName = GetJobName(context.BackgroundJob);
            var recurringJobId = GetRecurringJobIdIfApplicable(context.Connection, context.BackgroundJob.Id);
            var succeededState = context.CandidateState as SucceededState;
            if (succeededState != null)
            {
                SucceededJobTimestamps.WithLabels(jobName, recurringJobId, _deadline.ToString()).SetToCurrentTimeUtc();
            }
        }

        public void OnPerforming(PerformingContext context)
        {
            var jobName = GetJobName(context.BackgroundJob);
            var recurringJobId = GetRecurringJobIdIfApplicable(context.Connection, context.BackgroundJob.Id);
            context.Items["DurationTimer"] = JobDurationsTotal.WithLabels(jobName, recurringJobId).NewTimer();
        }

        public void OnPerformed(PerformedContext context)
        {
            if (!context.Items.ContainsKey("DurationTimer"))
            {
                throw new InvalidOperationException("No expected duration timer was found");
            }

            var timer = (IDisposable)context.Items["DurationTimer"];
            timer.Dispose();

            var jobName = GetJobName(context.BackgroundJob);
            var recurringJobId = GetRecurringJobIdIfApplicable(context.Connection, context.BackgroundJob.Id);
            JobDurationsCount.WithLabels(jobName, recurringJobId, _timelimit.ToString()).Inc();
        }
    }
}