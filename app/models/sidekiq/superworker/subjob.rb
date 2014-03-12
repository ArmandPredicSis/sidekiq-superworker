module Sidekiq
  module Superworker
    class Subjob
      include Mongoid::Document

      ATTRIBUTES = [
        :jid, :subjob_id, :superjob_id, :parent_id, :children_ids, :next_id,
        :subworker_class, :superworker_class, :arg_keys, :arg_values, :status,
        :descendants_are_complete, :meta
      ]

      ATTRIBUTES.each do |attr|
        field attr
      end

      validates_presence_of :subjob_id, :subworker_class, :superworker_class, :status

      def relatives
        self.class.where(superjob_id: superjob_id)
      end

      def parent
        return nil if parent_id.nil?
        relatives.where(subjob_id: parent_id).first
      end

      def children
        relatives.where(parent_id: subjob_id).order_by(:subjob_id => :desc)
      end

      def next
        relatives.where(subjob_id: next_id).first
      end

      def to_info
        "Subjob ##{id} (#{superworker_class} > #{subworker_class})"
      end
    end
  end
end
