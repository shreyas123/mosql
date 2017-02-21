module MoSQL
  module Logging
    def log
      @@logger ||= generate_log
    end

    def generate_log
      log = Log4r::Logger.new("Stripe::MoSQL")
      log.outputters << Log4r::Outputter.stdout
      log.outputters << Log4r::FileOutputter.new('logs', :filename =>  'logs.log')
      log
    end
  end
end
