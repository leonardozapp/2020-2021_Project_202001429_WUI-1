<?php
declare(strict_types=1);

namespace App\Model\Table;

use Cake\ORM\Query;
use Cake\ORM\RulesChecker;
use Cake\ORM\Table;
use Cake\Validation\Validator;

/**
 * TipoClasseFundos Model
 *
 * @property \App\Model\Table\CadastroFundosTable&\Cake\ORM\Association\HasMany $CadastroFundos
 *
 * @method \App\Model\Entity\TipoClasseFundo newEmptyEntity()
 * @method \App\Model\Entity\TipoClasseFundo newEntity(array $data, array $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[] newEntities(array $data, array $options = [])
 * @method \App\Model\Entity\TipoClasseFundo get($primaryKey, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo findOrCreate($search, ?callable $callback = null, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo patchEntity(\Cake\Datasource\EntityInterface $entity, array $data, array $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[] patchEntities(iterable $entities, array $data, array $options = [])
 * @method \App\Model\Entity\TipoClasseFundo|false save(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo saveOrFail(\Cake\Datasource\EntityInterface $entity, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[]|\Cake\Datasource\ResultSetInterface|false saveMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[]|\Cake\Datasource\ResultSetInterface saveManyOrFail(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[]|\Cake\Datasource\ResultSetInterface|false deleteMany(iterable $entities, $options = [])
 * @method \App\Model\Entity\TipoClasseFundo[]|\Cake\Datasource\ResultSetInterface deleteManyOrFail(iterable $entities, $options = [])
 */
class TipoClasseFundosTable extends Table
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

        $this->setTable('tipo_classe_fundos');
        $this->setDisplayField('classe');
        $this->setPrimaryKey('id');

        $this->hasMany('CadastroFundos', [
            'foreignKey' => 'tipo_classe_fundo_id',
        ]);
    }

    /**
     * Default validation rules.
     *
     * @param \Cake\Validation\Validator $validator Validator instance.
     * @return \Cake\Validation\Validator
     */
    public function validationDefault(Validator $validator): Validator
    {
        $validator
            ->integer('id')
            ->allowEmptyString('id', null, 'create');

        $validator
            ->scalar('classe')
            ->maxLength('classe', 100)
            ->requirePresence('classe', 'create')
            ->notEmptyString('classe');

        return $validator;
    }
}
