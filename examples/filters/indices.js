module.exports = {
  type:      'index',
  /**
   * Any indicies that trigger this predicate will be excluded from transfer
   * @param index - Full index configuration
   * @param arguments - The task-specific arguments object
   */
  predicate: (index) => index.name !== 'log_data_2016-12-01'
};