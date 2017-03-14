require File.join(File.dirname(__FILE__), '_lib.rb')

class MoSQL::Test::Functional::SQLTest < MoSQL::Test::Functional
  before do
    sequel.drop_table?(:test_upsert)
    sequel.create_table?(:test_upsert) do
      column :_id, 'INTEGER'
      column :color, 'TEXT'
      column :quantity, 'INTEGER'
      column :numbers, 'INTEGER ARRAY'
      column :jsonb_arrray, 'JSONB ARRAY'
      column :default, 'TEXT', default: 'default_val'
      column :text_array, 'TEXT ARRAY'
      primary_key [:_id]
    end

    @adapter = MoSQL::SQLAdapter.new(nil, sql_test_uri)
    @table = sequel[:test_upsert]
  end

  describe 'upsert' do
    it 'inserts new items' do
      @adapter.upsert!(@table, ['_id'], {'_id' => 0, 'color' => 'red', 'quantity' => 10, 'numbers' => Sequel.pg_array([1, 2, 3], :integer), 'jsonb_arrray' => Sequel.pg_array([JSON.dump({a: 1}), JSON.dump({a: 2})], :json), 'text_array' => Sequel.pg_array(["soy"], :text) })
      @adapter.upsert!(@table, ['_id'], {'_id' => 1, 'color' => 'blue', 'quantity' => 5, 'numbers' => Sequel.pg_array([], :integer), 'text_array' => Sequel.pg_array(["soy"], :text)})
      assert_equal(2, @table.count)
      assert_equal('red',  @table[:_id => 0][:color])
      assert_equal(10,     @table[:_id => 0][:quantity])
      assert_equal('blue', @table[:_id => 1][:color])
      assert_equal(5,      @table[:_id => 1][:quantity])
      assert_equal([1, 2, 3], @table[:_id => 0][:numbers])
      assert_equal([{"a"=>1}, {"a"=>2}], @table[:_id => 0][:jsonb_arrray])
      assert_equal(['soy'],      @table[:_id => 1][:text_array])
      assert_equal('default_val', @table[:_id => 0][:default])
      assert_equal([], @table[:_id => 1][:numbers])
    end

    it 'updates items' do
      @adapter.upsert!(@table, ['_id'], {'_id' => 0, 'color' => 'red', 'quantity' => 10})
      assert_equal(1, @table.count)
      assert_equal('red',  @table[:_id => 0][:color])

      @adapter.upsert!(@table, ['_id'], {'_id' => 0, 'color' => 'blue', 'quantity' => 5})
      assert_equal(1, @table.count)
      assert_equal('blue', @table[:_id => 0][:color])
    end
  end
end
