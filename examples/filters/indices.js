module.exports = {
  type: 'index',
  /**
   * Any indicies that trigger this predicate will be excluded from transfer
   * @param index - Full index configuration
   * @param arguments - The task-specific arguments object
   */
  predicate: (index, arguments) => index.name === arguments.targetName
;