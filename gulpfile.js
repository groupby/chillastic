/*eslint no-process-env: "off" */
/*eslint no-console: "off" */
const path      = require('path');
const gulp      = require('gulp');
const mocha     = require('gulp-mocha');
const eslint    = require('gulp-eslint');
const istanbul  = require('gulp-istanbul');
const gulpExit  = require('gulp-exit');
const coveralls = require('gulp-coveralls');
const gulpIf   = require('gulp-if');

const isFixed = (file) => {
  // Has ESLint fixed the file contents?
  return file.eslint != null && file.eslint.fixed;
};

gulp.task('test:dirty', () => {
  return gulp.src('tests/**/*.spec.js')
    .pipe(mocha({reporter: 'spec'}))
    .pipe(gulpExit());
});

const lint = () => {
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
    .pipe(eslint.failAfterError())
    .once('error', () => {
      console.error('lint failed');
      process.exit(1);
    })
    .pipe(gulpIf(isFixed, gulp.dest('.')))
    .once('end', () => {
      process.exit();
    });
};

gulp.task('lint', () => {
  return lint();
});

gulp.task('pre-test', () => {
  return gulp.src('app/**/*.js')
    .pipe(istanbul())
    .pipe(istanbul.hookRequire());
});

gulp.task('test:coverage', ['pre-test'], () => {
  return gulp.src(['tests/**/*.spec.js'])
    .pipe(mocha({reporter: 'spec'}))
    .pipe(istanbul.writeReports({
      reporters: [
        'text',
        'html',
        'lcov'
      ]
    }))
    .pipe(istanbul.enforceThresholds({
      thresholds: {
        lines:      80,
        branches:   65,
        functions:  80,
        statements: 80
      }
    }))
    .once('error', () => {
      console.error('coverage failed');
      process.exit(1);
    });
});

gulp.task('test:lint', ['test:coverage'], () => {
  return lint();
});

gulp.task('test', ['test:lint'], () => {
  return gulp.src(['*.js']);
});

gulp.task('coveralls', ['test'], () => {
  if (!process.env.CI) {
    return;
  }

  return gulp.src(path.join(__dirname, 'coverage/lcov.info'))
    .pipe(coveralls())
    .once('error', () => {
      console.error('coveralls failed');
      process.exit(1);
    })
    .once('end', () => {
      process.exit();
    });
});