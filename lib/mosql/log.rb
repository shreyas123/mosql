module MoSQL
  module Logging
    def log
      @@logger ||= generate_logger
    end

    def generate_logger
      log = Log4r::Logger.new("Stripe::MoSQL")
      log << Log4r::Outputter.stdout
      log << Log4r::FileOutputter.new('logs', :filename =>  'logs.log')
      log
    end
  end
end
