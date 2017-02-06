module MoSQL
  module Logging
    def log
      @@logger ||= Log4r::Logger.new("Stripe::MoSQL")
      @@logger.outputters << Log4r::Outputter.stdout
      @@logger.outputters << Log4r::FileOutputter.new('logs', :filename =>  'logs.log')
      @@logger
    end
  end
end
