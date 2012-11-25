$spec_dir = File.absolute_path(File.dirname(__FILE__))
require $spec_dir+"/helper"
$hosts = $spec_dir+"/hosts"

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.filter_run :focus
  config.run_all_when_everything_filtered = true
end

Helper.show=false

describe Helper do

  %w[ h V D n N P q g G T t v W X j ].each do |a|
    context "dir=001 arg=-#{a}" do
      subject { Helper.new("001","-"+a).run }
      it { should be_success }
    end
  end

  %w[ N q g G t v X j ].each do |a|
    context "dir=002 arg=-#{a}" do
      subject { Helper.new("002","-"+a).clean.run }
      it { should be_success }
      its(:n_files) { should eq 9 }
    end
  end

  if File.exist?($hosts)
    context "dir=002 --hostfile" do
      subject { Helper.new("002","-F ../hosts").clean.run }
      it { should be_success }
      its(:n_files) { should eq 9 }
    end
  end

  context "dir=003 w task argument" do
    subject { Helper.new("003","hello[foo,bar]").run }
    it { should be_success }
    its(:result) { should eq "foo,bar\nfoo,bar\n" }
  end

  context "dir=004 -j4 elapsed time" do
    subject { Helper.new("004","-j4").run }
    it { should be_success }
    its(:elapsed_time) { should be_within(1).of(4) }
  end

  if File.exist?($hosts)
    context "dir=005 --hostfile" do
      subject { Helper.new("005","-F ../hosts").run }
      it { should be_success }
      its("output_lines.sort") { should eq read_hosts($hosts).sort }
    end
  end

  context "dir=006 PASS_ENV" do
    subject { Helper.new("006","-F ../hosts ENV1=pass_successfully").run }
    it { should be_success }
    its(:result) { should eq "pass_successfully\n" }
  end

end
