<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * RelBenchmarksClasseFundos Model
 *
 * @property \App\Model\Table\TipoBenchmarksTable&\Cake\ORM\Association\BelongsTo $TipoBenchmarks
 * @property \App\Model\Table\TipoClasseFundosTable&\Cake\ORM\Association\BelongsTo $TipoClasseFundos
 *
 * @method \App\Model\Entity\RelBenchmarksClasseFundo newEmptyEntity()
 * @method \App\Model\Entity\RelBenchmarksClasseFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class RelBenchmarksClasseFundosTable extends Table
{
    /**
     * Initialize method
     *
     * @param array $config The configuration for the Table.
     * @return void
     */
    public function initialize(array $config): void
    {
        parent::initialize($config);

        $this->setTable('rel_benchmarks_classe_fundos');
        $this->setDisplayField('tipo_benchmark_id');
        $this->setPrimaryKey(['tipo_benchmark_id', 'tipo_classe_fundo_id']);

        $this->belongsTo('TipoBenchmarks', [
            'foreignKey' => 'tipo_benchmark_id',
            'joinType' => 'INNER',
        ]);
        $this->belongsTo('TipoClasseFundos', [
            'foreignKey' => 'tipo_classe_fundo_id',
            'joinType' => 'INNER',
        ]);
    }

    /**
     * Returns a rules checker object that will be used for validating
     * application integrity.
     *
     * @param \Cake\ORM\RulesChecker $rules The rules object to be modified.
     * @return \Cake\ORM\RulesChecker
     */
    public function buildRules(RulesChecker $rules): RulesChecker
    {
        $rules->add($rules->existsIn(['tipo_benchmark_id'], 'TipoBenchmarks'));
        $rules->add($rules->existsIn(['tipo_classe_fundo_id'], 'TipoClasseFundos'));

        return $rules;
    }
}
