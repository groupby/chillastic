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
    .pipe(istanbul.enforceThresholds({thresholds: {global: 78}}));
});

const lint = ()=> {
  return gulp.src([
    '**/*.js',
    '!node_modules/**',
    '!coverage/**'
  ])
    .pipe(eslint({
      fix: true
    }))
    .pipe(eslint.format())
    .pipe(eslint.failAfterError());
};

gulp.task('test:lint', ['test:coverage'], ()=> {
  return lint();
});

gulp.task('lint', ()=> {
  return lint();
});

gulp.task('test', ['test:lint'], ()=>{
  return gulp.src(['*.js']).pipe(gulpExit());
});