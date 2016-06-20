const express    = require('express');
const bodyParser = require('body-parser');
const controller = require('./root.controller');
const tasks      = require('./tasks');
const filters    = require('./filters');
const mutators   = require('./mutators');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.text());

const router = express.Router();
router.post('/_start', controller.start);
router.post('/_stop', controller.stop);
router.get('/status', controller.getStatus);

router.use('/tasks', tasks());
router.use('/filters', filters());
router.use('/mutators', mutators());

module.exports = ()=> router;
