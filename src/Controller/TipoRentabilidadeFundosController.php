<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoRentabilidadeFundos Controller
 *
 * @property \App\Model\Table\TipoRentabilidadeFundosTable $TipoRentabilidadeFundos
 * @method \App\Model\Entity\TipoRentabilidadeFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoRentabilidadeFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoRentabilidadeFundos = $this->paginate($this->TipoRentabilidadeFundos);

        $this->set(compact('tipoRentabilidadeFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Rentabilidade Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->get($id, [
            'contain' => ['CadastroFundos'],
        ]);

        $this->set(compact('tipoRentabilidadeFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->patchEntity($tipoRentabilidadeFundo, $this->request->getData());
            if ($this->TipoRentabilidadeFundos->save($tipoRentabilidadeFundo)) {
                $this->Flash->success(__('The tipo rentabilidade fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo rentabilidade fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoRentabilidadeFundo'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Rentabilidade Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->patchEntity($tipoRentabilidadeFundo, $this->request->getData());
            if ($this->TipoRentabilidadeFundos->save($tipoRentabilidadeFundo)) {
                $this->Flash->success(__('The tipo rentabilidade fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo rentabilidade fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoRentabilidadeFundo'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Rentabilidade Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoRentabilidadeFundo = $this->TipoRentabilidadeFundos->get($id);
        if ($this->TipoRentabilidadeFundos->delete($tipoRentabilidadeFundo)) {
            $this->Flash->success(__('The tipo rentabilidade fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo rentabilidade fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
