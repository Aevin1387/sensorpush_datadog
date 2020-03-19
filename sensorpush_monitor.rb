require 'active_record'
require 'active_support'
require 'active_support/core_ext/numeric/time'
require 'awesome_print'
require 'dogapi'
require 'pg'
require 'pry'
require 'rest-client'
require 'logger'


ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  database: 'sensorpush',
  username: 'sensorpush',
  password: ENV['SENSORPUSH_PG_PASSWORD']
)

class RecordedSample < ActiveRecord::Base
end
# CREATE TABLE recorded_samples (
#   id BIGSERIAL,
#   sampled_at TIMESTAMP NOT NULL,
#   sensor_id TEXT
# );
# CREATE UNIQUE INDEX ON recorded_samples(sampled_at);
# CREATE INDEX ON recorded_samples(sampled_at, sensor_id);
class SensorpushMonitor
  SENSORPUSH_AUTHORIZE_URL = 'https://api.sensorpush.com/api/v1/oauth/authorize'
  SENSORPUSH_ACCESS_TOKEN_URL = 'https://api.sensorpush.com/api/v1/oauth/accesstoken'
  SENSORPUSH_API = 'https://api.sensorpush.com/api/v1'
  def initialize(dd_api_key, dd_app_key, sp_username, sp_password, logger)
    @datadog_client = Dogapi::Client.new(dd_api_key, dd_app_key)
    @sp_username = sp_username
    @sp_password = sp_password
    @access_token = access_token
    @sensors = sensors
    @logger = logger
  end

  def reports
    begin
      list_response = RestClient.post(SENSORPUSH_API + '/reports/list', {}, headers=default_headers)
    rescue => e
      @logger.error(e)
      return []
    end
    JSON.parse(list_response.body)['files']
  end

  def process_all_reports
    all_reports = reports
    all_reports.each do |report|
      report = download_report(report['name'])
    end
  end

  def download_report(name)
    begin
      download_response = RestClient.post(SENSORPUSH_API + '/reports/download', {path:name}, headers=default_headers)
    rescue => e
      @logger.error(e)
      nil
    end
  end

  def get_samples(start_str, end_str, bulk=false)
    begin
      sample_response = RestClient.post(SENSORPUSH_API + '/samples', { "startTime" => start_str, "endTime" => end_str, 'bulk' => bulk }, headers=default_headers)
    rescue => e
      @logger.error(e)
      generic_response = {}
      generic_response.default = {}
      return generic_response
    end
    JSON.parse(sample_response.body)
  end

  def sensors
    begin
      sensors_response = RestClient.post(SENSORPUSH_API + '/devices/sensors', {  }, headers=default_headers)
    rescue => e
      @logger.error(e)
      generic_response = {}
      generic_response.default = {}
      return generic_response
    end
    JSON.parse(sensors_response.body)
  end

  def process_latest_samples
    latest_samples = get_samples(5.minutes.ago.to_datetime.to_s, DateTime.now.to_s)
    latest_samples['sensors'].keys.each do |sensor_id|
      process_sensor_samples(latest_samples['sensors'][sensor_id], sensor_id)
    end
  end

  def process_sensor_samples(sensor_samples, sensor_id)
    humidity_points, temperature_points = process_samples(sensor_samples, sensor_id)
    sensor = @sensors[sensor_id]
    return if sensor.nil?
    tags = ["sensor_id:#{sensor_id}", "sensor_name:#{sensor['name']}"]
    puts "Emitting sensorpush.relative_humidity: #{humidity_points}, sensor_id: #{sensor_id} sensor_name: #{sensor['name']}" if humidity_points.any?
    @datadog_client.emit_points('sensorpush.relative_humidity', humidity_points, tags: tags) if humidity_points.any?
    puts "Emitting sensorpush.temperature: #{temperature_points}, sensor_id: #{sensor_id} sensor_name: #{sensor['name']}" if temperature_points.any?
    @datadog_client.emit_points('sensorpush.temperature', temperature_points, tags: tags) if temperature_points.any?
  end

  def process_samples(samples, sensor_id)
    temperature_points = []
    humidity_points = []
    samples.each do |sample|
      observed_at = DateTime.parse(sample['observed'])
      recorded_sample = RecordedSample.where(sampled_at: observed_at, sensor_id: sensor_id).first
      next if recorded_sample
      humidity_points.push([observed_at.to_time, sample['humidity']])
      temperature_points.push([observed_at.to_time, sample['temperature']])
      RecordedSample.create(sampled_at: observed_at, sensor_id: sensor_id)
    end
    [humidity_points, temperature_points]
  end

  private

  def default_headers
    { "Authorization" => @access_token }
  end

  def access_token
    auth_response = RestClient.post(SENSORPUSH_AUTHORIZE_URL, { email: @sp_username, password: @sp_password })
    auth_response_parsed = JSON.parse(auth_response.body)
    authorization_key = auth_response_parsed['authorization']
    access_token_response = RestClient.post(SENSORPUSH_ACCESS_TOKEN_URL, { authorization: authorization_key })
    access_token_response_parsed = JSON.parse(access_token_response.body)
    access_token_response_parsed['accesstoken']
  end
end

logger = Logger.new(STDOUT)
sp = SensorpushMonitor.new ENV['DD_API_KEY'], ENV['DD_APP_KEY'], ENV['SENSORPUSH_EMAIL'], ENV['SENSORPUSH_PASSWORD'], logger

loop do
  begin
    sp.process_latest_samples
  rescue => e
    logger.error(e)
  end
  sleep 60
end