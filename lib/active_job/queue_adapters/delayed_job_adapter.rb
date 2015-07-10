require 'delayed_job'

module ActiveJob
  module QueueAdapters
    class DelayedJobAdapter
      class << self
        def enqueue(job)
          delayed_job = Delayed::Job.enqueue(JobWrapper.new(job.serialize), queue: job.queue_name)
          job.provider_job_id = delayed_job.id
          delayed_job
        end

        def enqueue_at(job, timestamp)
          delayed_job = Delayed::Job.enqueue(JobWrapper.new(job.serialize), queue: job.queue_name, run_at: Time.at(timestamp))
          job.provider_job_id = delayed_job.id
          delayed_job
        end
      end

      class JobWrapper #:nodoc:
        attr_accessor :job_data

        def initialize(job_data)
          @job_data = job_data
        end

        def perform
          Base.execute(job_data)
        end
      end
    end
  end
end
