const express    = require('express');
const controller = require('./tasks.controller');

const router = express.Router();
router.get('/', controller.getAll);

router.get('/:id/errors', controller.getErrors);
router.post('/:id', controller.add);
router.get('/:id', controller.get);
router.delete('/:id', controller.delete);

module.exports = ()=> router;
