var gulp = require('gulp');

gulp.task('copy_deps', function() {
    return gulp.src(['./node_modules/phaser/build/phaser.js', './node_modules/vertx3-eventbus-client/vertxbus.js', './node_modules/requirejs/require.js', './node_modules/sockjs/lib/sockjs.js'])
        .pipe(gulp.dest('./src/main/resources/webroot/js'));
});

gulp.task('build', ['copy_deps']);

