import d3 from './d3Importer';

export default class Loader {
  constructor() {
    this.logDump = d3.select('#logDump');
    this.csrftoken = d3.select('input[name=csrfmiddlewaretoken]').property('value');
    this.data = [];
    this.ids = [];
  }

  getLoaded() {
    return this.data.length;
  }

  /**
  * Given a list of selected sources, loads events and joins them with the currently loaded
  * to avoid duplicates.
  */
  async loadEvents(selectedSources) {
    try {
      console.log(`Fetching events for ${selectedSources.length} sources...`);
      const start = new Date();
      const data = await d3.json('/api/events_by_src/multi', {
        method: 'post',
        // credentials: 'same-origin',
        headers: {
          // Accept: 'application/json',
          // 'Content-Types': 'application/json',
          'X-CSRFToken': this.csrftoken,
        },
        body: JSON.stringify(selectedSources),
      });
      const ids = data.map((d) => d.eid);
      const delta = ids.filter((eid) => !this.ids.includes(eid));
      this.ids = [...this.ids, ...delta];
      this.data = [...this.data, ...data.filter((d) => delta.includes(d.eid))];
      this.logDump.selectAll('div')
        .data(this.data)
        .enter()
        .append('div')
        // .text((el) => `${el.eid}, ${el.date} [${el.sources}]`);
        .text((el) => `(${el.filename} - ${el.line}), ${el.eid} ${el.date}`);
      console.log(`Added ${delta.length} events, total: ${this.data.length} (took ${new Date() - start} ms)`);
      return this.data.length;
    } catch (error) {
      console.error(error);
      return -1;
    }
  }

  /**
   * Given a list of selected sources, unloads events if those don't belong to any other source
   * among the loaded sources.
   */
  unloadEvents(selectedSources, loadedSources) {
    const delta = [];
    const newData = [];
    this.data.forEach((d) => {
      const srcAfter = d.sources.filter(
        (src) => !selectedSources.includes(src) && loadedSources.includes(src),
      );
      if (srcAfter.length > 0) {
        newData.push(d);
      } else {
        delta.push(d.eid);
      }
    });
    this.data = newData;
    this.ids = this.data.map((d) => d.eid);
    this.logDump.selectAll('div')
      .data(this.data)
      .exit()
      .remove();
    console.log(`Removed ${delta.length} events, total: ${this.data.length}`);
    return this.data.length;
  }

  reset() {
    this.data = [];
    this.ids = [];
    this.logDump.selectAll('div')
      .data(this.data)
      .exit()
      .remove();
  }
}
