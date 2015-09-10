requirejs(["phaser", "vertxbus"], function(deps) {
    var STAND_F = "stand_f";
    var STAND_B = "stand_b";
    var STAND_L = "stand_l";
    var STAND_R = "stand_r";
    var ANIM_F = "anim_f";
    var ANIM_B = "anim_b";
    var ANIM_L = "anim_l";
    var ANIM_R = "anim_r";
    var MOV_LEFT = "l";
    var MOV_RIGHT = "r";
    var MOV_UP = "u";
    var MOV_DOWN = "d";

    var game;
    var gameDataInst;
    var cursors;
    var update_queue = [];
    var cmd = null;

    var gameId = parseInt(document.getElementById("gameid").value);
    var playerId = parseInt(document.getElementById("playerid").value);

    var eb = new vertx.EventBus('http://localhost:8070/eventbus');
    eb.onopen = function() {
        if(playerId == -1) {
            eb.registerHandler("browser.replay."+gameId, function(result) {
                if(result.type == "snapshot") {
                    gameDataInst = new gameData('map', gameId, playerId, result.players, result.round_id);
                    game = new Phaser.Game(800, 600, Phaser.AUTO, 'phaser-example', { preload: preload, create: create, update: update });
                }
                else {
                    update_queue.push(result)
                }
            });
            eb.send("replay.start."+gameId, {"id":parseInt(document.getElementById("spectatorid").value)});
        }
        else {
            eb.registerHandler("browser.game."+gameId, function(incoming) {
                update_queue.push(incoming)
            });
            eb.send("game."+gameId, {"action":"snp"}, function(result) {
                gameDataInst = new gameData('map', gameId, playerId, result.players, result.round_id);
                game = new Phaser.Game(800, 600, Phaser.AUTO, 'phaser-example', { preload: preload, create: create, update: update });
            });
        }

    };

    function preload() {
        game.load.tilemap('map', '/static/maps/map_1.json', null, Phaser.Tilemap.TILED_JSON);
        game.load.image('tiles_image', '/static/maps/map_1_tiles.bmp');
        game.load.spritesheet('mario', '/static/maps/sprite_sheet.png', 21, 21, 408);
    }

    function create() {
        game.stage.backgroundColor = '#787878';
        gameDataInst.init();
        cursors = game.input.keyboard.createCursorKeys();
    }

    function update() {
        player = gameDataInst.player();
        update_val = update_queue.shift();
        while(update_val) {
            //continue until we get current events
            if(update_val.round_id <= gameDataInst.roundId) {
                console.log("SKIPPING "+JSON.stringify(update_val));
                continue;
            }
            update_val.actions.forEach(function(action) {
                console.log("ADDING "+JSON.stringify(action));
                if(action.action == "newp") {
                    var pl = new character(action.player.player_id, action.player.x, action.player.y);
                    pl.init();
                    gameDataInst.players.set(pl.spriteId, pl);
                }
            });
            update_val.players.forEach(function(playerAction) {
                if(gameDataInst.playerId != -1 && cmd != null && player.spriteId == playerAction.player_id) {
                    cmd = null;
                }
                switch(playerAction.mov) {
                    case MOV_LEFT : gameDataInst.players.get(playerAction.player_id).moveLeft(); break;
                    case MOV_RIGHT : gameDataInst.players.get(playerAction.player_id).moveRight(); break;
                    case MOV_UP : gameDataInst.players.get(playerAction.player_id).moveUp(); break;
                    case MOV_DOWN : gameDataInst.players.get(playerAction.player_id).moveDown(); break;
                }
            });
            update_val = update_queue.shift();
        }

        if(gameDataInst.playerId != -1 && cmd == null) {
            if (cursors.left.isDown && gameDataInst.map.getTile(player.posX-1,player.posY,gameDataInst.walls) == null)
                cmd = {"action":"mov", "player_id":gameDataInst.playerId, "mov":MOV_LEFT};
            else if (cursors.right.isDown && gameDataInst.map.getTile(player.posX+1,player.posY,gameDataInst.walls) == null)
                cmd = {"action":"mov", "player_id":gameDataInst.playerId, "mov":MOV_RIGHT};
            else if (cursors.up.isDown && gameDataInst.map.getTile(player.posX,player.posY-1,gameDataInst.walls) == null)
                cmd = {"action":"mov", "player_id":gameDataInst.playerId, "mov":MOV_UP};
            else if (cursors.down.isDown && gameDataInst.map.getTile(player.posX,player.posY+1,gameDataInst.walls) == null)
                cmd = {"action":"mov", "player_id":gameDataInst.playerId, "mov":MOV_DOWN};
            if(cmd != null)
                eb.send("game."+gameDataInst.gameId, cmd);
        }

    }

    function gameData(mapName, gameId, playerId, playerStartInfo, roundId) {
        var me = this;
        this.mapName = mapName;
        this.gameId = gameId;
        this.playerId = playerId;
        this.roundId = roundId;

        this.players = new Map();
        playerStartInfo.forEach(function(entry){
            me.players.set(entry.player_id, new character(entry.player_id, entry.x, entry.y))
        });

        this.map = null;

        this.floor = null;
        this.furniture = null;
        this.walls = null;

        this.player = function() {
            return me.players.get(me.playerId);
        };

        this.init = function() {
            me.map = game.add.tilemap(mapName);
            me.map.addTilesetImage('tiles', 'tiles_image');
            me.floor = me.map.createLayer('floor');
            me.furniture = me.map.createLayer('furniture');
            me.walls = me.map.createLayer('walls');
            me.floor.resizeWorld();
            me.furniture.resizeWorld();
            me.walls.resizeWorld();
            me.players.forEach(function(entry) {
                console.log("INIT "+entry)
                entry.init();
            });
        }
    }

    function character(entityId,x, y) {
        var me = this;
        this.spriteId = entityId;
        this.spriteInstance = null;
        this.posX = x;
        this.posY = y;
        this.moveLeft = function() {me.posX -= 1; me.spriteInstance.x -= 20; me.spriteInstance.animations.play(ANIM_L, 5, true);};
        this.moveRight = function() {me.posX += 1; me.spriteInstance.x += 20; me.spriteInstance.animations.play(ANIM_R, 5, true);};
        this.moveUp = function() {me.posY -= 1; me.spriteInstance.y -= 20; me.spriteInstance.animations.play(ANIM_B, 5, true);};
        this.moveDown = function() {me.posY += 1; me.spriteInstance.y += 20; me.spriteInstance.animations.play(ANIM_F, 5, true);};
        this.standStill = function() {me.spriteInstance.animations.play(STAND_F, 5, true)};

        this.init = function() {
            me.spriteInstance = sprite(entityId, x, y);
            me.spriteInstance.animations.play(STAND_F, 5, true);
        }
    }

    function sprite(entityId, x, y) {
        spriteOffset = entityId*4;
        var sprite = game.add.sprite(x*gameDataInst.map.tileWidth, y*gameDataInst.map.tileHeight, "mario");
        sprite.animations.add(STAND_F, [spriteOffset]);
        sprite.animations.add(STAND_B, [spriteOffset+1]);
        sprite.animations.add(STAND_R, [spriteOffset+2]);
        sprite.animations.add(STAND_L, [spriteOffset+3]);

        sprite.animations.add(ANIM_F, [spriteOffset, spriteOffset+204]);
        sprite.animations.add(ANIM_B, [spriteOffset+1, spriteOffset+1+204]);
        sprite.animations.add(ANIM_R, [spriteOffset+2, spriteOffset+2+204]);
        sprite.animations.add(ANIM_L, [spriteOffset+3, spriteOffset+3+204]);
        return sprite;
    }


});