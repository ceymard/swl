"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Records are the objects that flow within our system
 */
class Data {
    constructor(collection, topology, data) {
        this.collection = collection;
        this.topology = topology;
        this.data = data;
    }
}
exports.Data = Data;
/**
 * Events flow alongside data in the pipes.
 */
class Event {
    constructor(type) {
        this.type = type;
    }
}
exports.Event = Event;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHlwZXMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvdHlwZXMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7QUFPQTs7R0FFRztBQUNIO0lBQ0UsWUFDUyxVQUFrQixFQUNsQixRQUFrQixFQUNsQixJQUFTO1FBRlQsZUFBVSxHQUFWLFVBQVUsQ0FBUTtRQUNsQixhQUFRLEdBQVIsUUFBUSxDQUFVO1FBQ2xCLFNBQUksR0FBSixJQUFJLENBQUs7SUFHbEIsQ0FBQztDQUNGO0FBUkQsb0JBUUM7QUFHRDs7R0FFRztBQUNIO0lBQ0UsWUFDUyxJQUFZO1FBQVosU0FBSSxHQUFKLElBQUksQ0FBUTtJQUdyQixDQUFDO0NBQ0Y7QUFORCxzQkFNQyJ9