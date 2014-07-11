/**
 * @todo (@imlucas) We should just make this a module rather than copy and pasting :p
 */
var gulp = require('gulp'),
  release = require('github-release'),
  pkg = require('./package.json'),
  browserify = require('browserify'),
  source = require('vinyl-source-stream'),
  rename = require('gulp-rename'),
  ecstatic = require('ecstatic'),
  http = require('http'),
  exec = require('child_process').exec;

var DIST = pkg.name + '.js';

function create(fn){
  browserify({entries: ['./bin/querybuilder.js']})
    .bundle({})
    .pipe(source('./bin/querybuilder.js'))
    .pipe(rename(DIST))
    .pipe(gulp.dest('./dist/'))
    .on('end', fn);
}

gulp.task('default', ['serve']);

gulp.task('build', create);

gulp.task('serve', ['build', 'watch'], function(){
  http.createServer(
    ecstatic({root: __dirname + '/'})
  ).listen(8080);
  exec('open http://localhost:8080');

  return gulp.src('./dist/' + DIST)
    .pipe(gulp.dest('./examples/'));
});

gulp.task('watch', function(){
  gulp.watch(['./lib/*.js'], ['build']);
});

gulp.task('dist', function(){
  create(function(){
    gulp.src('./dist/*').pipe(release(pkg));
  });
});

gulp.task('deploy', function(cb){
  var msg, cmd,
    src = __dirname,
    dest = 'build',
    remote = pkg.repository.url.replace('git://github.com/', 'git@github.com:');

  console.log('  getting last commit message');
  exec('git log --oneline HEAD | head -n 1', {cwd: src}, function(err, stdout){
    if(err) return cb(err);

    msg = stdout.toString();
    console.log('  last commit: ' + msg);
    cmd = [
      'git init',
      'rm -rf .DS_Store **/.DS_Store',
      'git add .',
      'git commit -m "Deploy: ' + msg + '"',
      'git push --force ' + remote + ' master:gh-pages',
      'rm -rf .git'
    ].join('&&');

    exec(cmd, {cwd: dest}, function(err){
      if(err) return cb(err);

      console.log('  ✔︎ GitHub Pages deployed');
      cb();
    });
  });
});
