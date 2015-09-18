var gulp = require('gulp');
var bower = require('gulp-bower');
var concat = require('gulp-concat');

gulp.task('bower', function() {
    return bower()
});

gulp.task('copy_deps', function() {
    return gulp.src(['./bower_components/phaser/build/phaser.js', './bower_components/vertx3-eventbus-client/vertxbus.js', './bower_components/requirejs/require.js', './bower_components/sockjs/sockjs.js'])
        .pipe(gulp.dest('./src/main/resources/webroot/js'));
});

gulp.task('build', ['bower', 'copy_deps']);
