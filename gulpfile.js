const gulp     = require('gulp');
const mocha    = require('gulp-mocha');
const eslint   = require('gulp-eslint');
const istanbul = require('gulp-istanbul');
const gulpExit = require('gulp-exit');

gulp.task('test:dirty', ()=> {
  return gulp.src('tests/**/*.spec.js')
    .pipe(mocha({reporter: 'spec'}))
    .pipe(gulpExit());
});

const lint = ()=> {
  return gulp.src([
    '**/*.js',
    '!node_modules/**',
    '!coverage/**',
    '!docker/**'
  ])
    .pipe(eslint({
      fix: true
    }))
    .pipe(eslint.format())
    .pipe(eslint.failAfterError());
};

gulp.task('lint', ()=> {
  return lint();
});

gulp.task('pre-test', ()=> {
  return gulp.src('app/**/*.js')
    .pipe(istanbul())
    .pipe(istanbul.hookRequire());
});

gulp.task('test:coverage', ['pre-test'], ()=> {
  return gulp.src(['tests/**/*.spec.js'])
    .pipe(mocha({reporter: 'spec'}))
    .pipe(istanbul.writeReports({
      reporters: [
        'text',
        'html'
      ]
    }))
    .pipe(istanbul.enforceThresholds({
      thresholds: {
        lines:      80,
        branches:   65,
        functions:  80,
        statements: 80
      }
    }));
});

gulp.task('xml-test:coverage', ['pre-test'], ()=> {
  return gulp.src(['tests/**/*.spec.js'])
    .pipe(mocha({
      reporter: 'mocha-junit-reporter',
      reporterOptions: {
        mochaFile: `${process.env.CIRCLE_TEST_REPORTS}/test-results.xml`
      }
    })) // Output XML for CircleCI
    .pipe(istanbul.writeReports({
      reporters: [
        'text',
        'html'
      ]
    }))
    .pipe(istanbul.enforceThresholds({
      thresholds: {
        lines:      80,
        branches:   65,
        functions:  80,
        statements: 80
      }
    }));
});

gulp.task('test:lint', ['test:coverage'], ()=> {
  return lint();
});

gulp.task('xml-test:lint', ['xml-test:coverage'], ()=> {
  return lint();
});

gulp.task('test', ['test:lint'], ()=> {
  return gulp.src(['*.js']).pipe(gulpExit());
});

gulp.task('xml-test', ['xml-test:lint'], ()=> {
  return gulp.src(['*.js']).pipe(gulpExit());
});