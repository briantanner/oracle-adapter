'use strict';

module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    mochaTest: {
      test: {
        src: ['test/**/*_test.js'],
        options: {
          reporter: grunt.option('reporter') || 'spec',
          require: 'config/blanket'
        }
      },
      coverage: {
        options: {
          reporter: 'html-cov',
          quiet: true,
          captureFile: 'coverage.html'
        },
        src: ['test/**/*_test.js']
      }
    },
    jshint: {
      options: {
        jshintrc: '.jshintrc'
      },
      gruntfile: {
        src: 'Gruntfile.js'
      },
      lib: {
        src: ['lib/**/*.js']
      },
      test: {
        src: ['test/**/*.js']
      }
    }
  });

  // These plugins provide necessary tasks.
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-jshint');

  // Default task.
  grunt.registerTask('default', ['jshint', 'mochaTest']);

  grunt.registerTask('doc', ['jshint','docker']);

};
