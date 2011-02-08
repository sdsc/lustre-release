#!/usr/bin/env ruby

require "yaml"
require "pp"


def main
    logdir=ARGV[0]
    File.open("#{logdir}/results.yml") do |yf|
        results = YAML::load(yf)
#        puts results["Tests"][0]["name"]
        
        results["Tests"].each do |t|
            passed = 0
            t["SubTests"].each do |sub|
                if sub["status"] != "PASS" then
                    puts "#{t["name"]} #{sub["name"]} #{sub["status"]}"
                else
                    passed = passed + 1
                end
            end
            puts "#{t["name"]} #{passed} subtests passed"
        end
    end
end        

main

